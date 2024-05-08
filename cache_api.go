package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"k8s.io/klog"
)

type PodsAndPVsCache struct {
	Clientset     *kubernetes.Clientset
	CsiDriverName string
}

type PodEntry struct {
	name      string
	namespace string
	phase     v1.PodPhase
}

var podCacheMux sync.Mutex
var podsByUid map[string]PodEntry

var pvsCacheMux sync.Mutex

// true if the PV is created by the driver
var pvStatusByName map[string]bool

func newPodsAndPVsCache(
	clientSet *kubernetes.Clientset,
	csiProvisionerName string) *PodsAndPVsCache {
	return &PodsAndPVsCache{
		clientSet,
		csiProvisionerName,
	}
}

func (podsAndPVsCache *PodsAndPVsCache) registerPodsWatch() {
	stopCh := make(chan struct{})
	defer close(stopCh)
	factory := informers.NewSharedInformerFactory(
		podsAndPVsCache.Clientset,
		5*time.Minute)
	defer runtime.HandleCrash()
	podInformer := factory.Core().V1().Pods().Informer()
	pvInformer := factory.Core().V1().PersistentVolumes().Informer()
	podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    onAddPod,
		UpdateFunc: onUpdatePod,
		DeleteFunc: onDeletePod,
	})
	pvInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    podsAndPVsCache.onAddPV,
		UpdateFunc: podsAndPVsCache.onUpdatePV,
		DeleteFunc: podsAndPVsCache.onDeletePV,
	})
	factory.Start(stopCh)
	if !cache.WaitForCacheSync(stopCh, podInformer.HasSynced, pvInformer.HasSynced) {
		klog.Error("Timed out waiting for caches to sync")
		runtime.HandleError(fmt.Errorf("timed out waiting for caches to sync"))
		return
	}
	<-stopCh
}

func onAddPod(addedPod interface{}) {
	pod := addedPod.(*v1.Pod)
	podCacheMux.Lock()
	podsByUid[string(pod.UID)] = PodEntry{name: pod.Name, namespace: pod.Namespace, phase: pod.Status.Phase}
	podCacheMux.Unlock()
	klog.Infof("Pod %s/%s with uid %v and status %v is added to cache", pod.Namespace, pod.Name, pod.UID, pod.Status.Phase)
}

func (podsAndPVsCache *PodsAndPVsCache) onAddPV(addedPV interface{}) {
	pv := addedPV.(*v1.PersistentVolume)
	if !podsAndPVsCache.isCsiDriverVolume(pv) {
		return
	}
	pvsCacheMux.Lock()
	pvStatusByName[pv.Name] = true
	pvsCacheMux.Unlock()
	klog.Infof("PV %s is added to cache", pv.Name)
}

func onUpdatePod(oldPod interface{}, updatedPod interface{}) {
	old := oldPod.(*v1.Pod)
	updated := updatedPod.(*v1.Pod)
	if old.UID == updated.UID && old.Status.Phase == updated.Status.Phase {
		return
	}
	onDeletePod(oldPod)
	// Add after delete so that if the new uid is same, we do not remove recent update
	onAddPod(updatedPod)
}

func (podsAndPVsCache *PodsAndPVsCache) onUpdatePV(oldPV interface{}, updatedPV interface{}) {
	old := oldPV.(*v1.PersistentVolume)
	updated := oldPV.(*v1.PersistentVolume)
	if podsAndPVsCache.isCsiDriverVolume(old) {
		podsAndPVsCache.onDeletePV(old)
	}
	if podsAndPVsCache.isCsiDriverVolume(updated) {
		// Add after delete so that if the new uid is same, we do not remove recent update
		podsAndPVsCache.onAddPV(updated)
	}
}

func onDeletePod(deletedPod interface{}) {
	pod := deletedPod.(*v1.Pod)
	podCacheMux.Lock()
	delete(podsByUid, string(pod.UID))
	podCacheMux.Unlock()
	klog.Infof("Pod %s/%s with uid %v is removed from the cache", pod.Namespace, pod.Name, pod.UID)
}

func (podsAndPVsCache *PodsAndPVsCache) onDeletePV(deletedPV interface{}) {
	pv := deletedPV.(*v1.PersistentVolume)
	if !podsAndPVsCache.isCsiDriverVolume(pv) {
		return
	}
	pvsCacheMux.Lock()
	delete(pvStatusByName, pv.Name)
	pvsCacheMux.Unlock()
	klog.Infof("PV %s is removed from the cache", pv.Name)
}

func (podsAndPVsCache *PodsAndPVsCache) Run() {
	podsByUid = make(map[string]PodEntry)
	pvStatusByName = make(map[string]bool)
	klog.Info("Launched Pods and PVs cache API")
	go podsAndPVsCache.launchControllerApi()
	klog.Info("Registering pods and PVs watcher")
	podsAndPVsCache.registerPodsWatch()
}

func (podsAndPVsCache *PodsAndPVsCache) launchControllerApi() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		handler(w, r, podsAndPVsCache.CsiDriverName)
	})
	if err := http.ListenAndServe(":8080", nil); err != nil {
		klog.Fatalf("Failed server start due to %v", err)
	}
}

func handler(w http.ResponseWriter, req *http.Request, csiDriverName string) {
	if req.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Write([]byte("Unsupported method"))
		return
	}
	contentType := req.Header.Get("Content-Type")
	if len(contentType) == 0 || strings.ToLower(contentType) != "application/json" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Unsupported content type. Required application/json type"))
		return
	}
	defer req.Body.Close()
	body, err := io.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		klog.Errorf("Failed reading request body due to %s", err)
		return
	}
	var request ResolvePodsRequest
	err = json.Unmarshal(body, &request)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		klog.Errorf("Failed converting request body to resolve pods request due to %s", err)
		return
	}
	if request.CsiDriverName != csiDriverName {
		w.WriteHeader(http.StatusBadRequest)
		klog.Errorf("Expected %s driver name but got %s", csiDriverName, request.CsiDriverName)
		return
	}
	var resolvedPods []ResolvedPod
	for _, staleMount := range request.StaleMounts {
		pvsCacheMux.Lock()
		for _, pv := range staleMount.PvNames {
			if _, ok := pvStatusByName[pv]; ok { // cache has matching PV - provisioned by the CSI driver
				podCacheMux.Lock()
				if pod, ok := podsByUid[staleMount.PodUid]; ok {
					resolvedPods = append(resolvedPods, ResolvedPod{Name: pod.name, Namespace: pod.namespace, Uid: staleMount.PodUid})
				} else {
					klog.Infof("Pod with uid %s is not found in the cache", staleMount.PodUid)
				}
				podCacheMux.Unlock()
			} else {
				klog.Infof("PV %s is not found in the cache (cache is not up-to-date yet/PV is not provisioned by %s)", pv, csiDriverName)
			}
		}
		pvsCacheMux.Unlock()
	}
	resp := ResolvePodsResponse{resolvedPods}
	if respBytes, err := json.Marshal(resp); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		klog.Errorf("Failed marshalling response")
		return
	} else {
		w.WriteHeader(http.StatusOK)
		w.Write(respBytes)
	}
}

func (podsAndPVsCache *PodsAndPVsCache) isCsiDriverVolume(pv *v1.PersistentVolume) bool {
	return pv.Spec.CSI != nil && pv.Spec.CSI.Driver == podsAndPVsCache.CsiDriverName
}
