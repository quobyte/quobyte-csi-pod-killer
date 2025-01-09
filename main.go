package main

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"k8s.io/client-go/rest"
	"k8s.io/klog"
)

type Role string

const (
	Cache   Role = "cache"
	Monitor Role = "monitor" // runs on each node and monitors CSI mount points
)

/*
* kubeconfig is only reqired to run pod killer as a binary
* example: ./pod_killer --kubeconfig ~/.kube/config --driver_name="csi.quobyte.com" --node_name="k8s-1" -v=3
 */
var (
	// common for both monitor and controller
	role       Role
	kubeconfig = flag.String("kubeconfig", "", "kubeconfig file (if not provided, uses in-cluster configuration)")

	// For pod killer controller
	csiProvisionerName = flag.String("driver_name", "", "CSI provisioner name (must match the CSI provisioner name)")

	// For monitoring stale pod mounts on node
	monitoringInterval     = flag.Duration("monitoring_interval", 5*time.Second, "monitoring interval")
	nodeName               = flag.String("node_name", "", "K8S node name")
	serviceUrl             = flag.String("service_url", "", "Pod killer controller service URL")
	parallelKills          = flag.Int("parallel_kills", 10, "Kill 'n' pods with stale mount points")
)

func main() {
	flag.Func("role", "Driver role (controller or monitor)", func(flagValue string) error {
		if len(flagValue) == 0 {
			return fmt.Errorf("-role is required")
		}
		switch strings.ToLower(flagValue) {
		case "cache":
			role = Cache
			return nil
		case "monitor":
			role = Monitor
			return nil
		default:
			return fmt.Errorf("unknown role value %s", flagValue)
		}
	})
	klog.InitFlags(nil)
	flag.Set("alsologtostderr", "true")
	flag.Parse()

	var config *rest.Config
	var err error

	if len(*csiProvisionerName) == 0 {
		klog.Fatal("--driver_name is required")
	}
	if *kubeconfig != "" {
		config, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
		if err != nil {
			klog.Fatalf("Could not create k8s API configuration for the given kubeconfig %v.", err)
		}
	} else {
		config, err = rest.InClusterConfig()
		if err != nil {
			klog.Fatalf("Failed to retrieve in-cluster configuration due to %v.", err)
		}
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Fatalf("Failed to create rest client due to %v.", err)
	}

	if role == Cache {
		podsAndPVsCache := newPodsAndPVsCache(
			clientset,
			*csiProvisionerName)
		podsAndPVsCache.Run()
	} else if role == Monitor {
		if len(*nodeName) == 0 {
			klog.Fatal("--node_name is required")
		}
		if len(*serviceUrl) == 0 {
			klog.Fatal("--service_url is required")
		}
		klog.Infof("Start monitoring of stale pod mounts every %s on node %s and use %s to resolve pod name/namespace",
			*monitoringInterval, *nodeName, *serviceUrl)
		monitor := &CsiMountMonitor{
			clientSet:              clientset,
			csiDriverName:          *csiProvisionerName,
			nodeName:               *nodeName,
			monitoringInterval:     *monitoringInterval,
			controller_url:         *serviceUrl,
			parallelKills:          *parallelKills,
			podDeletionQueue:       NewPodDeletionQueue(),
		}
		monitor.Run()
	} else {
		klog.Fatalf("Unexpected role %v", role)
	}
}
