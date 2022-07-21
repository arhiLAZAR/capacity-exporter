package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/spf13/pflag"
	"gopkg.in/yaml.v3"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	metricsv "k8s.io/metrics/pkg/client/clientset/versioned"
)

const (
	defaultConfigPath = "/app/config.yaml"
	DEBUG             = true
)

type configType struct {
	Affinity []struct {
		Key      string
		Value    string
		Operator v1.NodeSelectorOperator
	}

	AllDeploymentsPrefix string `yaml:"all_deployments_prefix"`
	AllDeploymentsSuffix string `yaml:"all_deployments_suffix"`

	Namespaces []struct {
		Name               string
		Frontend           bool
		DeploymentAlias    string   `yaml:"deployment_alias"`
		DeploymentPrefix   string   `yaml:"deployment_prefix"`
		DeploymentSuffix   string   `yaml:"deployment_suffix"`
		DependsOn          []string `yaml:"depends_on"`
		DependsOnFullChain []string
	}
}

type deploymentLabelsType struct {
	Allowed   []allowedLabelsType
	Forbidden []forbiddenLabelsType
}

type allowedLabelsType struct {
	Key    string
	Values []string
}

type forbiddenLabelsType struct {
	Key    string
	Values []string
}

func main() {
	config := readConfig()

	nodeList := getNodeList()

	for _, namespace := range config.Namespaces {

		deploymentName := getDeploymentName(&config, namespace.Name)
		deploymentLabels := getAntiAffinityLabels(&config, namespace.Name, deploymentName)

		printDebug("Namespace: \"%s\"\nAllowed labels: %+v\nForbidden labels: %+v\n", namespace.Name, deploymentLabels.Allowed, deploymentLabels.Forbidden)

		totalAllocatableCPU, totalAllocatableMemory := getTotalAllocatableResources(deploymentLabels, &nodeList)
		printDebug("Allocatable MilliCpuSum: %+v\nAllocatable MemSum: %+v\n", totalAllocatableCPU, totalAllocatableMemory)

		dependencies := getDependencies(&config, namespace.Name)
		printDebug("Dependencies: %+v\n\n", dependencies)
	}

	totalUsedCPU, totalUsedMemory := getUsedResources("", "", "")
	printDebug("\nUsed MilliCpuSum: %+v\nnUsed MemSum: %+v\n", totalUsedCPU, totalUsedMemory)

}

// Get total amount of allocatable memory and cpu
// If allowed labels are specified then count the node only if labels match
// If forbidden labels are specified then count the node only if labels do not match
func getTotalAllocatableResources(deploymentLabels deploymentLabelsType, nodeList *v1.NodeList) (int64, int64) {
	var everythingAllowed, nothingForbidden, thisNodeIsAllowed bool
	var cpuSum, memSum int64

	if len(deploymentLabels.Allowed) > 0 {
		everythingAllowed = false
	} else {
		everythingAllowed = true
	}

	if len(deploymentLabels.Forbidden) > 0 {
		nothingForbidden = false
	} else {
		nothingForbidden = true
	}

	for _, node := range nodeList.Items {
		thisNodeIsAllowed = false

		if everythingAllowed && nothingForbidden {
			printDebug("Sum all nodes")
			thisNodeIsAllowed = true
		} else {

			if !everythingAllowed && !nothingForbidden {
				thisNodeIsForbidden := false

				for nodeLabelKey, nodeLabelValue := range node.Labels {
					printDebug("Nodelabel: %+v, check both-forbidden", nodeLabelKey)
					for _, forbiddenLabel := range deploymentLabels.Forbidden {
						// Do not count this node if the node and the deployment has the same FORBIDDEN label...
						if nodeLabelKey == forbiddenLabel.Key {
							for _, forbiddenLabelValue := range forbiddenLabel.Values {
								// ...with the same value
								if nodeLabelValue == forbiddenLabelValue {
									thisNodeIsForbidden = true
								}
							}
						}
					}
					printDebug("\t%v\n", thisNodeIsAllowed)
				} // End of "range node.Labels"

				if !thisNodeIsForbidden {
					for nodeLabelKey, nodeLabelValue := range node.Labels {
						printDebug("Nodelabel: %+v, check both-allowed", nodeLabelKey)
						for _, allowedLabel := range deploymentLabels.Allowed {
							// Count this node if the node and the deployment has the same ALLOWED label...
							if nodeLabelKey == allowedLabel.Key {
								for _, allowedLabelValue := range allowedLabel.Values {
									// ...with the same value
									if nodeLabelValue == allowedLabelValue {
										thisNodeIsAllowed = true
									}
								}
							}
						}
						printDebug("\t%v\n", thisNodeIsAllowed)
					} // End of "range node.Labels"
				}

			} else {

				if !everythingAllowed {
					for nodeLabelKey, nodeLabelValue := range node.Labels {
						printDebug("Nodelabel: %+v, check allowed", nodeLabelKey)
						for _, allowedLabel := range deploymentLabels.Allowed {
							// Count this node if the node and the deployment has the same ALLOWED label...
							if nodeLabelKey == allowedLabel.Key {
								for _, allowedLabelValue := range allowedLabel.Values {
									// ...with the same value
									if nodeLabelValue == allowedLabelValue {
										thisNodeIsAllowed = true
									}
								}
							}
						}
						printDebug("\t%v\n", thisNodeIsAllowed)
					} // End of "range node.Labels"
				}

				if !nothingForbidden {
					thisNodeIsAllowed = true
					for nodeLabelKey, nodeLabelValue := range node.Labels {
						printDebug("Nodelabel: %+v, check forbidden", nodeLabelKey)
						for _, forbiddenLabel := range deploymentLabels.Forbidden {
							// Do not count this node if the node and the deployment has the same FORBIDDEN label...
							if nodeLabelKey == forbiddenLabel.Key {
								for _, forbiddenLabelValue := range forbiddenLabel.Values {
									// ...with the same value
									if nodeLabelValue == forbiddenLabelValue {
										thisNodeIsAllowed = false
									}
								}
							}
						}
						printDebug("\t%v\n", thisNodeIsAllowed)
					} // End of "range node.Labels"
				}
			}
		}

		if thisNodeIsAllowed {
			printDebug("The node %+v is allowed!\n", node.Name)

			cpuSum += node.Status.Capacity.Cpu().MilliValue()
			memSum += node.Status.Capacity.Memory().Value()
		}

	} // End of "range nodeList.Items"

	return cpuSum, memSum
}

// Get a list of all nodes in the cluster
func getNodeList() v1.NodeList {
	clientset := getMetaV1Clientset()

	nodeList, err := clientset.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
	checkErr(err)

	return *nodeList
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
				printDebug("\tPREFIX\t")
			}

			if strings.HasSuffix(container.Name, suffix) {
				printDebug("\tSUFFIX\t")
			}

			printDebug("MilliCpu: %+v\t", container.Usage.Cpu().MilliValue())
			printDebug("Mem: %+v\n", container.Usage.Memory().Value())

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

// Combine deployment name from prefix, suffix and namespace name (or alias)
func getDeploymentName(config *configType, targetNamespace string) string {
	var baseName, prefix, suffix string

	prefix = config.AllDeploymentsPrefix
	suffix = config.AllDeploymentsSuffix

	for _, currentNamespace := range config.Namespaces {
		if currentNamespace.Name == targetNamespace {

			if currentNamespace.DeploymentPrefix != "" {
				if currentNamespace.DeploymentPrefix == "UNSET" {
					prefix = ""
				} else {
					prefix = currentNamespace.DeploymentPrefix
				}
			}

			if currentNamespace.DeploymentSuffix != "" {
				if currentNamespace.DeploymentSuffix == "UNSET" {
					suffix = ""
				} else {
					suffix = currentNamespace.DeploymentSuffix
				}
			}

			if currentNamespace.DeploymentAlias != "" {
				baseName = currentNamespace.DeploymentAlias
			} else {
				baseName = currentNamespace.Name
			}
		}
	}

	finalName := fmt.Sprintf("%s%s%s", prefix, baseName, suffix)
	return finalName
}

// Check if the deployment in the specified namespace has some affinities
func getAntiAffinityLabels(config *configType, namespace, deploymentName string) deploymentLabelsType {
	var deploymentLabels deploymentLabelsType

	clientset := getMetaV1Clientset()

	deploymentList, err := clientset.AppsV1().Deployments(namespace).List(context.TODO(), metav1.ListOptions{})
	checkErr(err)

	for _, deployment := range deploymentList.Items {

		if deployment.Name == deploymentName {

			specAffinity := deployment.Spec.Template.Spec.Affinity
			if specAffinity != nil && specAffinity.NodeAffinity != nil && specAffinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {

				NodeSelectorTerms := deployment.Spec.Template.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
				for _, nodeSelectorTerm := range NodeSelectorTerms {

					for _, deploymentAffinity := range nodeSelectorTerm.MatchExpressions {

						for _, configAffinity := range config.Affinity {

							if deploymentAffinity.Key == configAffinity.Key && deploymentAffinity.Operator == configAffinity.Operator {

								if deploymentAffinity.Operator == "In" {
									var labels allowedLabelsType
									labels.Key = deploymentAffinity.Key
									labels.Values = deploymentAffinity.Values

									deploymentLabels.Allowed = append(deploymentLabels.Allowed, labels)
								}
								if deploymentAffinity.Operator == "NotIn" {
									var labels forbiddenLabelsType
									labels.Key = deploymentAffinity.Key
									labels.Values = deploymentAffinity.Values
									deploymentLabels.Forbidden = append(deploymentLabels.Forbidden, labels)
								}

							}

						}

					}

				}

			}

		}

	}

	return deploymentLabels
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
		printDebug("%s", output)
	}
	return output
}

func printDebug(line string, variable ...interface{}) {
	if DEBUG {
		fmt.Printf(line, variable...)
	}
}

func checkErr(err error) {
	if err != nil {
		// panic(err)
		fmt.Println(err)
	}
}
