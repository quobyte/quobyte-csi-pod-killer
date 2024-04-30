package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	"k8s.io/apimachinery/pkg/util/runtime"
	internalinterfaces "k8s.io/client-go/informers/internalinterfaces"

	"k8s.io/klog"
)

type PodKiller struct {
	Clientset          *kubernetes.Clientset
	CSIProvisionerName string
	NodeName           string
	parallelKills      int
}

const (
	KUBELET_PODS_MOUNT_PATH = "/var/lib/kubelet/pods/"
	// Example /var/lib/kubelet/pods/<pod-uid>/volumes/kubernetes.io~csi/
	KUBELET_POD_CSI_MOUNTS = KUBELET_PODS_MOUNT_PATH + "%s/volumes/kubernetes.io~csi/"
	// /var/lib/kubelet/pods/<pod-uid>/volumes/kubernetes.io~csi/<pv-name>/mount
	KUBELET_POD_CSI_PVC_MOUNT = KUBELET_POD_CSI_MOUNTS + "%s/mount"

	WATCH_LIMIT = 2
)

type PodEntry struct {
	name      string
	namespace string
	phase     v1.PodPhase
}

var podCacheMux sync.Mutex
var podsByUid map[string]PodEntry

func newPodKiller(
	clientSet *kubernetes.Clientset,
	csiProvisionerName string,
	nodeName string,
	parallelKills int) *PodKiller {
	return &PodKiller{
		clientSet,
		csiProvisionerName,
		nodeName,
		parallelKills,
	}
}

type StaleMount struct {
	podUid  string
	pvNames []string
}

func (podKiller *PodKiller) registerPodsWatch() {
	stopCh := make(chan struct{})
    defer close(stopCh)
	factory := informers.NewSharedInformerFactoryWithOptions(
		podKiller.Clientset,
		10 * time.Minute,
		informers.WithTweakListOptions(NodeNameFilter(podKiller.NodeName)))
	defer runtime.HandleCrash()
	go factory.Start(stopCh)
	podInformer := factory.Core().V1().Pods().Informer()
    if !cache.WaitForCacheSync(stopCh, podInformer.HasSynced) {
        runtime.HandleError(fmt.Errorf("timed out waiting for caches to sync"))
        return
    }
	podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
        AddFunc: onAdd,
        UpdateFunc: onUpdate,
        DeleteFunc: onDelete,
    })

    <-stopCh
}


func NodeNameFilter(nodeName string) internalinterfaces.TweakListOptionsFunc {
	return func(l *metav1.ListOptions) {
		if l == nil {
			l = &metav1.ListOptions{}
		}
		l.FieldSelector = l.FieldSelector + "spec.nodeName=" + nodeName
	}
}

func onAdd(addedPod interface{}) {
	pod := addedPod.(*v1.Pod)
	podCacheMux.Lock()
	podsByUid[string(pod.UID)] = PodEntry{name: pod.Name, namespace: pod.Namespace, phase: pod.Status.Phase}
	podCacheMux.Unlock()
	klog.Infof("pod %s/%s with uid %v and status %v is added to cache", pod.Namespace, pod.Name, pod.UID, pod.Status.Phase)
}

func onUpdate(oldPod interface{}, updatedPod interface{}) {
	old := oldPod.(*v1.Pod)
	updated := updatedPod.(*v1.Pod)
	if old.UID == updated.UID && old.Status.Phase == updated.Status.Phase {
		return
	}
	onDelete(oldPod)
	// Add after delete so that if the new uid is same, we do not remove recent update
	onAdd(updatedPod)
}

func onDelete(deletedPod interface{}) {
	pod := deletedPod.(*v1.Pod)
	podCacheMux.Lock()
	delete(podsByUid, string(pod.UID))
	podCacheMux.Unlock()
	klog.Infof("pod %s/%s with uid %v is removed from the cache", pod.Namespace, pod.Name, pod.UID)
}

func walkAndQueueStaleCsiMounts(staleMountsQueue chan<- StaleMount) {
	for {
		if pods, err := getChildDirectoryNames(KUBELET_PODS_MOUNT_PATH); err != nil {
			klog.Errorf("failed listing kubelet pod mounts due to %s", err)
		} else {
			for _, podUid := range pods {
				stalePVMounts := getStalePVNames(podUid)
				if len(stalePVMounts) > 0 {
					staleMountsQueue <- StaleMount{podUid: podUid, pvNames: stalePVMounts}
				}
			}
		}
		time.Sleep(*monitoringInterval)
	}
}

func (podKiller *PodKiller) Run(monitoringInterval *time.Duration) {
	podsByUid = make(map[string]PodEntry)
	staleMountsQueue := make(chan StaleMount, podKiller.parallelKills * 2)
	go walkAndQueueStaleCsiMounts(staleMountsQueue)

	for i := 0; i < podKiller.parallelKills; i++ {
		go podKiller.deletePodWithStaleMountPoint(staleMountsQueue)
	}
	podKiller.registerPodsWatch()
}

func (podKiller *PodKiller) deletePodWithStaleMountPoint(staleMountsQueue <-chan StaleMount) {
	for staleMount := range staleMountsQueue {
		klog.V(5).Infof("found a stale mount for pod uid %s in the kubelet path %", staleMount.podUid, KUBELET_PODS_MOUNT_PATH)
		for _, pvName := range staleMount.pvNames {
			if pv, err := podKiller.Clientset.CoreV1().PersistentVolumes().Get(context.Background(), pvName, metav1.GetOptions{}); err == nil {
				if !podKiller.isCsiDriverVolume(pv) {
					klog.Infof("%v is not provisioned by the Driver %s", pv, podKiller.CSIProvisionerName)
					// not provisioned by the given CSI Driver, so try with other PVs
					continue
				}
				podCacheMux.Lock()
				pod, ok := podsByUid[staleMount.podUid]
				podCacheMux.Unlock()
				if ok {
					if pod.phase == v1.PodPending {
						klog.V(5).Infof("pod %s/%s is in pending state. Skipping pod deletion.", pod.namespace, pod.name)
						break
					}
					klog.Infof("Deleting pod %s/%s due to stale mount point(s) %v on the node", pod.namespace, pod.name, staleMount.pvNames)
					// Looks like not have grace period does not trigger watches - so cache entries are not removed
					gracePeriod := int64(60)
					err := podKiller.Clientset.CoreV1().Pods(pod.namespace).Delete(context.TODO(), pod.name, metav1.DeleteOptions{
						GracePeriodSeconds: &gracePeriod,
					})
					if err != nil {
						klog.Errorf("failed deletion of pod %s with stale mount point %s due to %s", staleMount.podUid, pvName, err)
					}
					break
				} else {
					klog.Errorf("pod %s not found in cache. Skipping delete (will delete once the cache entry is populated)", staleMount.podUid)
				}
			} else {
				klog.Errorf("failed to get PV %s of Pod %s with error: %s", pvName, staleMount.podUid, err)
			}
		}
	}
}

func (podKiller *PodKiller) isCsiDriverVolume(pv *v1.PersistentVolume) bool {
	return pv.Spec.CSI != nil && pv.Spec.CSI.Driver == podKiller.CSIProvisionerName
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
	for _, csiPVName := range csiPvNames {
		var stat unix.Stat_t
		quobyteCsiVolumePath := fmt.Sprintf(KUBELET_POD_CSI_PVC_MOUNT, podUid, csiPVName)
		if err = unix.Stat(quobyteCsiVolumePath, &stat); err != nil {
			klog.V(2).Infof("Encountered error %d executing stat on %s", err.(syscall.Errno), quobyteCsiVolumePath)
			if err.(syscall.Errno) == unix.ENOTCONN || err.(syscall.Errno) == unix.ENOENT {
				stalePvNames = append(stalePvNames, csiPVName)
			}
		}
	}
	if len(stalePvNames) > 0 {
		klog.Infof("PVs %v are stale in the path %s", stalePvNames, podVolumesPath)
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
