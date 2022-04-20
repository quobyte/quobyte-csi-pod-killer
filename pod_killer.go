package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"k8s.io/klog"
)

type PodKiller struct {
	Clientset          *kubernetes.Clientset
	CSIProvisionerName string
	NodeName           string
}

func newPodKiller(
	clientSet *kubernetes.Clientset,
	csiProvisionerName string,
	nodeName string) *PodKiller {
	return &PodKiller{
		clientSet,
		csiProvisionerName,
		nodeName,
	}
}

func (podKiller *PodKiller) Run(monitoringInterval *time.Duration) {
	for {
		if pvs, err := podKiller.Clientset.CoreV1().PersistentVolumes().List(context.TODO(), metav1.ListOptions{}); err == nil {
			if pods, err := podKiller.Clientset.CoreV1().Pods("").List(context.TODO(), metav1.ListOptions{FieldSelector: "spec.nodeName=" + podKiller.NodeName}); err == nil {
				provisionerPVs := podKiller.filterProvisionerPVs(pvs)
				for _, pod := range pods.Items {
					if pod.Spec.NodeName != *nodeName {
						klog.V(2).Infof("pod %s/%s is not running on node %s", pod.Namespace, pod.Name, *nodeName)
						continue
					}

					if pod.Status.Phase == v1.PodPending {
						klog.V(2).Infof("pod %s/%s is in pending state. Skipping pod.", pod.Namespace, pod.Name)
						continue
					}

					if podKiller.hasInaccessibleQuobyteVolumes(pod, provisionerPVs) {
						klog.Infof("triggering delete for pod %s/%s due to inaccessible Quobyte CSI volume", pod.Namespace, pod.Name)
						gracePeriod := int64(0)
						if err := podKiller.Clientset.CoreV1().Pods(pod.Namespace).Delete(context.TODO(), pod.Name, metav1.DeleteOptions{
							GracePeriodSeconds: &gracePeriod,
						}); err != nil {
							klog.Errorf("unable to delete pod %s/%s due to %v", pod.Namespace, pod.Name, err)
						}
					}
				}
			} else {
				klog.Errorf("failed to get pod list due to %v", err)
			}
		} else {
			klog.Errorf("failed to get PV list due to %v", err)
		}
		time.Sleep(*monitoringInterval)
	}
}

func (podKiller *PodKiller) filterProvisionerPVs(pvs *v1.PersistentVolumeList) map[string]v1.PersistentVolume {
	provisionerPVs := make(map[string]v1.PersistentVolume)
	for _, pv := range pvs.Items {
		if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == podKiller.CSIProvisionerName {
			provisionerPVs[pv.Name] = pv
		}
	}
	return provisionerPVs
}

func (podKiller *PodKiller) hasInaccessibleQuobyteVolumes(pod v1.Pod, pvs map[string]v1.PersistentVolume) bool {
	// kubernetes pod volumes are mounted on host at
	// /var/lib/kubelet/pods/<Pod-ID>/volumes/kubernetes.io~csi/<PV-Name>/mount
	podVolumesPath := fmt.Sprintf("/var/lib/kubelet/pods/%s/volumes/kubernetes.io~csi", string(pod.UID))
	if dirExists, err := exists(podVolumesPath); !dirExists {
		if err != nil {
			klog.V(2).Infof("CSI volume mount path for the pod %s/%s (%s) not found due to %v", pod.Namespace, pod.Name, string(pod.UID), err)
		} else {
			klog.V(2).Infof("Pod %s/%s (namespace/name) with unique id %s do not have any CSI volumes", pod.Namespace, pod.Name, string(pod.UID))
		}
		return false
	}

	csiVolumes, err := getDirs(podVolumesPath)
	if err != nil {
		klog.Errorf("skipping pod deletion: unable to find CSI volumes for the pod %s/%s (%s) due to %v", pod.Namespace, pod.Name, string(pod.UID), err)
		return false
	}
	for _, csiPVName := range csiVolumes {
		if _, ok := pvs[csiPVName]; ok { // quobyte CSI volume
			var stat unix.Stat_t
			quobyteCsiVolumePath := podVolumesPath + "/" + csiPVName + "/mount"
			if err = unix.Stat(quobyteCsiVolumePath, &stat); err != nil {
				klog.V(2).Infof("Encountered error %d executing stat on %s", err.(syscall.Errno), quobyteCsiVolumePath)
				if err.(syscall.Errno) == unix.ENOTCONN || err.(syscall.Errno) == unix.ENOENT {
					return true
				}
			}
		}
	}
	return false
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

func getDirs(podPath string) ([]string, error) {
	var mountedVolumes []string
	dirs, err := ioutil.ReadDir(podPath)
	if err != nil {
		klog.Errorf("Failed to get CSI volume from the pod mount path %s due to %v", podPath, err)
		return nil, err
	}
	for _, dir := range dirs {
		if dir.IsDir() {
			mountedVolumes = append(mountedVolumes, dir.Name())
		}
	}
	return mountedVolumes, nil
}
