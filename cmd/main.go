package main

import (
	"fmt"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	metricsv "k8s.io/metrics/pkg/client/clientset/versioned"
)

func main() {

}

func getMetaV1Clientset(apiVersion ...string) *kubernetes.Clientset {
	config, err := rest.InClusterConfig()
	checkErr(err)

	clientset, err := kubernetes.NewForConfig(config)
	checkErr(err)

	return clientset
}

func getMetricsClientset(apiVersion ...string) *metricsv.Clientset {
	config, err := rest.InClusterConfig()
	checkErr(err)

	clientset, err := metricsv.NewForConfig(config)
	checkErr(err)

	return clientset
}

func checkErr(err error) {
	if err != nil {
		// panic(err)
		fmt.Println(err)
	}
}
