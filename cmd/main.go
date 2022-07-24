package main

import (
	"fmt"
	"io/ioutil"

	"github.com/spf13/pflag"
	"gopkg.in/yaml.v3"
	v1 "k8s.io/api/core/v1"
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
	Allowed   []allowedAndForbiddenLabelsType
	Forbidden []allowedAndForbiddenLabelsType
}

type allowedAndForbiddenLabelsType struct {
	Key    string
	Values []string
}

func main() {
	config := readConfig()

	nodeList := getNodeList()
	podList := getPodList()

	for _, namespace := range config.Namespaces {

		deploymentName := getDeploymentName(&config, namespace.Name)
		deploymentLabels := getAntiAffinityLabels(&config, namespace.Name, deploymentName)

		printDebug("Namespace: \"%s\"\nAllowed labels: %+v\nForbidden labels: %+v\n", namespace.Name, deploymentLabels.Allowed, deploymentLabels.Forbidden)

		allocatableCPU, allocatableMemory, allowedNodes := getAllocatableResources(deploymentLabels, &nodeList)
		printDebug("Allocatable MilliCpuSum: %+v\nAllocatable MemSum: %+v\nAllowed nodes: %+v\n", allocatableCPU, allocatableMemory, allowedNodes)

		totalRequestedCPU, totalRequestedMemory := getTotalRequestedResources(allowedNodes, &podList)
		printDebug("Total Requested MilliCpuSum: %+v\nTotal Requested MemSum: %+v\n", totalRequestedCPU, totalRequestedMemory)

		deploymentRequestedCPU, deploymentRequestedMemory := getDeploymentRequestedResources(namespace.Name, deploymentName)
		printDebug("Deployment Requested MilliCpuSum: %+v\nDeployment Requested MemSum: %+v\n", deploymentRequestedCPU, deploymentRequestedMemory)

		podsAmount := len(getPodList(namespace.Name).Items)
		printDebug("Amount of pods: %+v\n", podsAmount)

		dependencies := getDependencies(&config, namespace.Name)
		printDebug("Dependencies: %+v\n\n", dependencies)
	}

	totalUsedCPU, totalUsedMemory := getUsedResources("", "", "")
	printDebug("\nUsed MilliCpuSum: %+v\nnUsed MemSum: %+v\n", totalUsedCPU, totalUsedMemory)

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

func getAllNamespaces(config *configType) []string {
	var namespaceList []string

	for _, namespace := range config.Namespaces {
		namespaceList = append(namespaceList, namespace.Name)
	}
	return namespaceList
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

func inList(variable string, list []string) bool {
	for _, x := range list {
		if x == variable {
			return true
		}
	}
	return false
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
