package main

type StaleMount struct {
	PodUid  string   `json:"podUid"`
	PvNames []string `json:"pvNames"`
}

type ResolvePodsRequest struct {
	StaleMounts   []StaleMount `json:"staleMounts"`
	NodeName      string       `json:"nodeName"`
	CsiDriverName string       `json:"csiDriverName"`
}

type ResolvedPod struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	Uid       string `json:"uid"`
}

type ResolvePodsResponse struct {
	Pods []ResolvedPod `json:"pods"`
}

type ResolvedPodWithStaleMounts struct {
	ResolvedPod ResolvedPod
	StaleMount  StaleMount
}
