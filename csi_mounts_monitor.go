package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog"
)

const (
	KUBELET_PODS_MOUNT_PATH = "/var/lib/kubelet/pods/"
	// Example /var/lib/kubelet/pods/<pod-uid>/volumes/kubernetes.io~csi/
	KUBELET_POD_CSI_MOUNTS = KUBELET_PODS_MOUNT_PATH + "%s/volumes/kubernetes.io~csi/"
	// /var/lib/kubelet/pods/<pod-uid>/volumes/kubernetes.io~csi/<pv-name>/mount
	KUBELET_POD_CSI_PVC_MOUNT  = KUBELET_POD_CSI_MOUNTS + "%s/mount"
	QUOBYTE_CLIENT_X_ATTR      = "quobyte.statuspage_port"
	CLIENT_X_ATTR_VALUE_SIZE   = 100
	TOTAL_BATCHING_WAIT_BUDGET = 1000 * time.Millisecond
)

type podDeletionQueue struct {
	podsMux sync.Mutex
	pods    map[string]bool
}

func NewPodDeletionQueue() *podDeletionQueue {
	return &podDeletionQueue{
		pods: make(map[string]bool),
	}
}

func (podCache *podDeletionQueue) add(podUid string) bool {
	podCache.podsMux.Lock()
	defer podCache.podsMux.Unlock()
	if _, ok := podCache.pods[podUid]; ok {
		return false // not added
	}
	podCache.pods[podUid] = true
	return true
}

func (podCache *podDeletionQueue) delete(podUid string) {
	podCache.podsMux.Lock()
	defer podCache.podsMux.Unlock()
	delete(podCache.pods, podUid)
}

type CsiMountMonitor struct {
	csiDriverName          string
	clientSet              *kubernetes.Clientset
	controller_url         string
	nodeName               string
	monitoringInterval     time.Duration
	parallelKills          int
	podUidResolveBatchSize int
	podDeletionQueue       *podDeletionQueue
}

func (csiMountMonitor *CsiMountMonitor) Run() {
	// Keep Queue >= 2 * podUidResolveBatchSize so that we batch resolve pod uid calls
	staleMountsChannel := make(chan StaleMount, csiMountMonitor.podUidResolveBatchSize*3)
	resolvedPodsChannel := make(chan ResolvedPodWithStaleMounts, csiMountMonitor.parallelKills*3)
	go csiMountMonitor.walkAndDetectStaleMounts(staleMountsChannel)
	go csiMountMonitor.resolvePodsWithStaleMounts(staleMountsChannel, resolvedPodsChannel)
	for i := 0; i < csiMountMonitor.parallelKills; i++ {
		go csiMountMonitor.deletePodWithStaleMount(resolvedPodsChannel)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	wg.Wait()
}

func (csiMountMonitor *CsiMountMonitor) deletePodWithStaleMount(resolvedPodsChannel <-chan ResolvedPodWithStaleMounts) {
	for resolvedPodAndMounts := range resolvedPodsChannel {
		podNamespace := resolvedPodAndMounts.ResolvedPod.Namespace
		podName := resolvedPodAndMounts.ResolvedPod.Name
		podUid := resolvedPodAndMounts.ResolvedPod.Uid
		klog.Infof("Deleting pod %s/%s with uid %s due to stale mounts %s",
			podNamespace,
			podName,
			podUid,
			resolvedPodAndMounts.StaleMount.PvNames,
		)
		gracePeriod := int64(0)
		if err := csiMountMonitor.clientSet.CoreV1().Pods(podNamespace).Delete(context.Background(), podName, metav1.DeleteOptions{
			GracePeriodSeconds: &gracePeriod,
			Preconditions:      &metav1.Preconditions{UID: (*types.UID)(&podUid)},
		}); err != nil {
			klog.Errorf("Unable to delete pod %s/%s (uid: %s) due to %v", podNamespace, podName, podUid, err)
		}
		csiMountMonitor.podDeletionQueue.delete(podUid)
	}
}

func (csiMountMonitor *CsiMountMonitor) resolvePodsWithStaleMounts(
	staleMountsChannel <-chan StaleMount,
	resolvedPodsChannel chan<- ResolvedPodWithStaleMounts) {
	for staleMount := range staleMountsChannel { // Blocks if no elements
		batch := make([]StaleMount, 0, csiMountMonitor.podUidResolveBatchSize)
		batch = append(batch, staleMount)
		currentBatchingDelay := 0 * time.Millisecond
		for i := 1; i < csiMountMonitor.podUidResolveBatchSize; i++ {
			select { // non-blocking with default
			case staleMount := <-staleMountsChannel:
				batch = append(batch, staleMount)
			default:
				if currentBatchingDelay < TOTAL_BATCHING_WAIT_BUDGET {
					time.Sleep(100 * time.Millisecond)
					currentBatchingDelay += 100 * time.Millisecond
				}
			}
		}
		if resolvedPods, err := csiMountMonitor.resolvePods(batch); err != nil {
			for _, staleMount := range batch { // remove and let it be requeued for resolution
				csiMountMonitor.podDeletionQueue.delete(staleMount.PodUid)
			}
			klog.Errorf("Could not resolve pod(s) to name/namespace due to %s. Will retry later again.", err)
		} else {
			resolvedPodUids := make(map[string]bool)
			for _, pod := range resolvedPods.Pods {
				klog.V(2).Infof("Resolved pod uid %s to %s/%s", pod.Uid, pod.Namespace, pod.Name)
				resolvedPodsChannel <- ResolvedPodWithStaleMounts{pod, staleMount}
				resolvedPodUids[pod.Uid] = true
			}
			if len(resolvedPodUids) == 0 {
				klog.Infof("No pods with Quobyte volume as stale mount points")
			}
			for _, staleMount := range batch { // pod killer cache may not have entry yet
				if _, ok := resolvedPodUids[staleMount.PodUid]; !ok {
					// Pod was not resolved, so let it be requeued again
					csiMountMonitor.podDeletionQueue.delete(staleMount.PodUid)
				}
			}
		}
	}
}

func (csiMountMonitor *CsiMountMonitor) resolvePods(staleMounts []StaleMount) (ResolvePodsResponse, error) {
	resolvePodsToDelete := &ResolvePodsRequest{
		StaleMounts:   staleMounts,
		NodeName:      csiMountMonitor.nodeName,
		CsiDriverName: csiMountMonitor.csiDriverName}
	var reqBodyJson []byte
	var err error
	var resolvedPods ResolvePodsResponse
	if reqBodyJson, err = json.Marshal(resolvePodsToDelete); err != nil {
		return resolvedPods, err
	}
	client := &http.Client{}
	if req, err := http.NewRequest(http.MethodPost, csiMountMonitor.controller_url, bytes.NewBuffer(reqBodyJson)); err != nil {
		return resolvedPods, err
	} else {
		req.Header.Add("Content-Type", "application/json")
		if resp, err := client.Do(req); err != nil {
			return resolvedPods, err
		} else {
			if resp.StatusCode != 200 {
				return resolvedPods, fmt.Errorf("unexpected error code %d while resolving pod uid", resp.StatusCode)
			}
			defer resp.Body.Close()
			responseBytes, err := io.ReadAll(resp.Body)
			if err != nil {
				return resolvedPods, err
			}

			if err = json.Unmarshal(responseBytes, &resolvedPods); err != nil {
				return resolvedPods, err
			}
			return resolvedPods, nil
		}
	}
}

func (csiMountMonitor *CsiMountMonitor) walkAndDetectStaleMounts(staleMountsChannel chan<- StaleMount) {
	for {
		podsWithStaleMountsCounter := 0
		if pods, err := getChildDirectoryNames(KUBELET_PODS_MOUNT_PATH); err != nil {
			klog.Errorf("Failed listing kubelet pod mounts due to %s", err)
		} else {
			for _, podUid := range pods {
				stalePVMounts := getStalePVNames(podUid)
				if len(stalePVMounts) > 0 {
					if added := csiMountMonitor.podDeletionQueue.add(podUid); !added {
						// Having entry in this queue does not mean we delete the pod, we do not have enough
						// information to know if pod has Quobyte volumes mounted. We know this information
						// once we resolve pods using cache_api.go
						klog.V(5).Infof("Pod %s already exists in deletion queue. Not adding again.", podUid)
						continue // Already queued this pod for deletion - skip in this round
					}
					podsWithStaleMountsCounter += 1
					klog.Infof("Found pod %s with stale mount path(s) %s. Pod will be deleted if mount path is backed by Quobyte volume.",
						podUid,
						stalePVMounts)
					staleMountsChannel <- StaleMount{PodUid: podUid, PvNames: stalePVMounts}
				} else {
					klog.V(2).Infof("No stale mounts found for the pod %s", podUid)
				}
			}
		}
		if podsWithStaleMountsCounter == 0 {
			klog.Infof("No pods with stale mounts found during the monitoring run.")
		} else {
			klog.Infof("Found %d pods with stale mounts during the monitoring run.", podsWithStaleMountsCounter)
		}
		time.Sleep(csiMountMonitor.monitoringInterval)
	}
}

func getStalePVNames(podUid string) []string {
	podVolumesPath := fmt.Sprintf(KUBELET_POD_CSI_MOUNTS, podUid)
	if dirExists, err := exists(podVolumesPath); !dirExists {
		if err != nil {
			klog.V(2).Infof("CSI volume mount path for the pod %s not found. Skipping pod", podUid)
		}
		return nil
	}
	// Get all the CSI volume dirs for the pod (cannot identify driver specific volume here)
	csiPvNames, err := getChildDirectoryNames(podVolumesPath)
	if err != nil {
		klog.Errorf("Unable to list PVs from pod mount path %s due to %v", podVolumesPath, err)
		return nil
	}
	klog.V(5).Infof("Loaded %v PV Names from the path %s", csiPvNames, podVolumesPath)
	var stalePvNames []string
	xattr_buf := make([]byte, CLIENT_X_ATTR_VALUE_SIZE)
	for _, csiPVName := range csiPvNames {
		quobyteCsiVolumePath := fmt.Sprintf(KUBELET_POD_CSI_PVC_MOUNT, podUid, csiPVName)
		if _, err = unix.Getxattr(quobyteCsiVolumePath, QUOBYTE_CLIENT_X_ATTR, xattr_buf); err != nil {
			klog.V(2).Infof("Encountered error %d executing stat on %s", err.(syscall.Errno), quobyteCsiVolumePath)
			if err.(syscall.Errno) == unix.ENOTCONN {
				stalePvNames = append(stalePvNames, csiPVName)
			}
		}
	}
	return stalePvNames
}

func exists(dirPath string) (bool, error) {
	_, err := os.Stat(dirPath)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func getChildDirectoryNames(podPath string) ([]string, error) {
	var resultDirs []string
	dirs, err := os.ReadDir(podPath)
	if err != nil {
		klog.Errorf("Failed to get CSI volume from the pod mount path %s due to %v", podPath, err)
		return nil, err
	}
	for _, dir := range dirs {
		if dir.IsDir() {
			resultDirs = append(resultDirs, dir.Name())
		}
	}
	return resultDirs, nil
}
