package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"time"

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
						klog.V(2).Infof("pod %s/%s is not running on %s", pod.Namespace, pod.Name, *nodeName)
						continue
					}

					if podKiller.hasInaccessibleQuobyteVolumes(pod, provisionerPVs) {
						klog.Infof("trigger delete for pod %s/%s due to inaccessible Quobyte CSI volume", pod.Namespace, pod.Name)
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
    if pv.Spec.CSI != nil &&  pv.Spec.CSI.Driver == podKiller.CSIProvisionerName {
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
			klog.V(2).Infof("CSI volume mount path for the pod %s/%s (%s) not found", pod.Namespace, pod.Name, string(pod.UID))
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
			csiVolumePath := podVolumesPath + "/" + csiPVName + "/mount"
			klog.V(2).Infof("accessing xattr from %s to determine the mount availability", csiVolumePath)
			opts := []string{"-n", "quobyte.statuspage_port", csiVolumePath}
			if _, err := exec.Command("getfattr", opts...).CombinedOutput(); err != nil {
				klog.Errorf("could not access CSI volume %s for pod %s/%s (%s) due to %v.", csiVolumePath, pod.Namespace, pod.Name, string(pod.UID), err)
				return true
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
