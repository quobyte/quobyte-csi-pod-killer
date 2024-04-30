package main

import (
	"flag"
	"os"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"k8s.io/client-go/rest"
	"k8s.io/klog"
)

/*
* kubeconfig is only reqired to run pod killer as a binary
* example: ./pod_killer --kubeconfig ~/.kube/config --driver_name="csi.quobyte.com" --node_name="k8s-1" -v=3
 */
var (
	kubeconfig         = flag.String("kubeconfig", "", "kubeconfig file (if not provided, uses in-cluster configuration)")
	monitoringInterval = flag.Duration("monitoring_interval", 5*time.Second, "monitoring interval")
	csiProvisionerName = flag.String("driver_name", "", "CSI provisioner name (must match the CSI provisioner name)")
	nodeName           = flag.String("node_name", "", "K8S node name")
	parallelKills      = flag.Int("parallel_kills", 10, "Kill 'n' pods with stale mount points")
)

func main() {
	klog.InitFlags(nil)
	flag.Set("alsologtostderr", "true")
	flag.Parse()

	var config *rest.Config
	var err error

	if len(*csiProvisionerName) == 0 {
		klog.Errorf("Fatal: --driver_name is required")
		os.Exit(1)
	}

	if len(*nodeName) == 0 {
		klog.Errorf("Fatal: --node_name is required")
		os.Exit(1)
	}

	if *kubeconfig != "" {
		config, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
		if err != nil {
			klog.Errorf("Fatal: Could not create k8s API configuration for the given kubeconfig %v.", err)
			os.Exit(1)
		}
	} else {
		config, err = rest.InClusterConfig()
		if err != nil {
			klog.Errorf("Fatal: Failed to retrieve in-cluster configuration due to %v.", err)
			os.Exit(1)
		}
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Errorf("Fatal: Failed to create rest client due to %v.", err)
		os.Exit(1)
	}
	podKiller := newPodKiller(clientset, *csiProvisionerName, *nodeName, *parallelKills)
	klog.Infof("start monitoring of pods every %s on node %s for csi provisioner %s", *monitoringInterval, *nodeName, *csiProvisionerName)
	podKiller.Run(monitoringInterval)
}
