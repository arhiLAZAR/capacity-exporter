package main

import (
	"context"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	metricsv "k8s.io/metrics/pkg/client/clientset/versioned"
)

func main() {
	totalUsedCpu, totalUsedMemory := getUsedResources("", "", "")
	fmt.Printf("\nMilliCpuSum: %+v\nMemSum: %+v\n", totalUsedCpu, totalUsedMemory)

}

// Count amount of used Cpu and Memory for specified namespace and deployment prefix and suffix
func getUsedResources(namespace string, affixes ...string) (int64, int64) {

	prefix := ""
	suffix := ""

	if len(affixes) > 0 {
		prefix = affixes[0]
	}

	if len(affixes) > 1 {
		suffix = affixes[1]
	}

	clientset := getMetricsClientset()

	podMetricsList, err := clientset.MetricsV1beta1().PodMetricses(namespace).List(context.TODO(), metav1.ListOptions{})
	checkErr(err)

	var cpuSum, memSum int64

	for _, pod := range podMetricsList.Items {
		for _, container := range pod.Containers {

			printWithTabs(container.Name, 3)

			if strings.HasPrefix(container.Name, prefix) {
				fmt.Printf("\tPREFIX\t")
			}

			if strings.HasSuffix(container.Name, suffix) {
				fmt.Printf("\tSUFFIX\t")
			}

			fmt.Printf("MilliCpu: %+v\t", container.Usage.Cpu().MilliValue())
			fmt.Printf("Mem: %+v\n", container.Usage.Memory().Value())

			cpuSum += container.Usage.Cpu().MilliValue()
			memSum += container.Usage.Memory().Value()
		}
	}

	return cpuSum, memSum
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

func printWithTabs(str string, indent int) {
	fmt.Printf("%s\t", str)
	for i := 1; i < indent-len(str)/8; i++ {
		fmt.Printf("\t")
	}
}

func checkErr(err error) {
	if err != nil {
		// panic(err)
		fmt.Println(err)
	}
}
