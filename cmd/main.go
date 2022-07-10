package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/spf13/pflag"
	"gopkg.in/yaml.v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	metricsv "k8s.io/metrics/pkg/client/clientset/versioned"
)

const defaultConfigPath = "/app/config.yaml"

type configType struct {
	Affinity []struct {
		Key      string
		Value    string
		Operator string
	}
	Namespaces []struct {
		Name               string
		Frontend           bool
		DeploymentAlias    string   `yaml:"deployment_alias"`
		DependsOn          []string `yaml:"depends_on"`
		DependsOnFullChain []string
	}
}

func main() {
	config := readConfig()

	for _, namespace := range config.Namespaces {
		dependencies := getDependencies(&config, namespace.Name)

		fmt.Printf("Namespace %s has the following dependencies: %+v\n", printWithTabs(namespace.Name, 2, false), dependencies)
	}

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

// Gather all dependencies and sub-dependencies of one namespace
func getDependencies(config *configType, suzerain string, suzerainList ...string) []string {
	var vassalList []string

	suzerainList = append(suzerainList, suzerain)
	allNamespaces := getAllNamespaces(config)

	for _, currentNamespace := range config.Namespaces {
		if currentNamespace.Name == suzerain {
			for _, vassal := range currentNamespace.DependsOn {
				if inList(vassal, suzerainList) {
					panic("Dependency loop detected!")
				}

				if !inList(vassal, allNamespaces) {
					panic("Found undescribed dependency: " + vassal)
				}

				vassalList = append(vassalList, vassal)
				vassalList = append(vassalList, getDependencies(config, vassal, suzerainList...)...)
			}
		}
	}

	return vassalList
}

func getAllNamespaces(config *configType) []string {
	var namespaceList []string

	for _, namespace := range config.Namespaces {
		namespaceList = append(namespaceList, namespace.Name)
	}
	return namespaceList
}

func inList(variable string, list []string) bool {
	for _, x := range list {
		if x == variable {
			return true
		}
	}
	return false
}

func readConfig() configType {
	// Parse a config flag
	configPath := pflag.StringP("config", "c", defaultConfigPath, "Path to config file")
	pflag.Parse()

	configData, err := ioutil.ReadFile(*configPath)
	checkErr(err)

	config := &configType{}
	err = yaml.Unmarshal(configData, config)
	checkErr(err)

	return *config
}

func printWithTabs(str string, indent int, printOutput ...bool) string {
	tabs := "\t"

	for i := 1; i < indent-len(str)/8; i++ {
		tabs += "\t"
	}

	output := str + tabs

	if len(printOutput) == 0 || printOutput[0] {
		fmt.Printf("%s", output)
	}
	return output
}

func checkErr(err error) {
	if err != nil {
		// panic(err)
		fmt.Println(err)
	}
}
