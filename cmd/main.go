package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"time"

	promapi "github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/spf13/pflag"
	"gopkg.in/yaml.v3"
	v1 "k8s.io/api/core/v1"
)

const (
	defaultConfigPath        = "/app/config.yaml"
	DEBUG                    = true
	prometheusDefaultTimeout = 10
)

type configType struct {
	Prometheus struct {
		Address       string
		Timeout       int64
		QueryTemplate string `yaml:"query_template"`
	}

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
		Prometheus         struct {
			QueryVariable     string `yaml:"query_variable"`
			QueryFullOverride string `yaml:"query_full_override"`
		}
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

type promQueryParamsType struct {
	QueryTime   time.Time
	PromTimeout time.Duration
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

		usedCPU, usedMemory := getUsedResources(namespace.Name, deploymentName)
		printDebug("Used MilliCpuSum: %+v\nnUsed MemSum: %+v\n", usedCPU, usedMemory)

		dependencies := getDependencies(&config, namespace.Name)
		printDebug("Dependencies: %+v\n", dependencies)

		rps := getRPS(&config, namespace.Name)
		printDebug("RPS: %+v\n\n", rps)
	}

}

// Get Requests Per Second for the specified namespace (from Prometheus)
func getRPS(config *configType, namespace string) float64 {

	promAddress := config.Prometheus.Address
	promQuery := parsePromQuery(config, namespace)

	promResponse := promRequest(promAddress, promQuery)

	if len(promResponse) == 0 {
		return 0
	}
	return promResponse[0]
}

func parsePromQuery(config *configType, targetNamespace string) string {
	var outputQuery string

	for _, currentNamespace := range config.Namespaces {
		if currentNamespace.Name == targetNamespace {
			if currentNamespace.Prometheus.QueryFullOverride != "" {
				outputQuery = currentNamespace.Prometheus.QueryFullOverride
			} else {
				outputQuery = fmt.Sprintf(config.Prometheus.QueryTemplate, currentNamespace.Prometheus.QueryVariable)
			}

		}
	}

	return outputQuery
}

// Get values for the provided Prometheus query
func promRequest(address, query string, params ...promQueryParamsType) []float64 {
	var actualParams promQueryParamsType
	var response []float64

	printDebug("Prom query: %s\n", query)

	if len(params) == 0 {
		actualParams.PromTimeout = prometheusDefaultTimeout * time.Second
		actualParams.QueryTime = time.Now()
	} else {

		if params[0].QueryTime.IsZero() {
			actualParams.QueryTime = time.Now()
		} else {
			actualParams.QueryTime = params[0].QueryTime
		}

		if params[0].PromTimeout == time.Duration(0) {
			actualParams.PromTimeout = prometheusDefaultTimeout * time.Second
		} else {
			actualParams.PromTimeout = params[0].PromTimeout * time.Second
		}

	}

	client, err := promapi.NewClient(promapi.Config{Address: address})
	checkErr(err)

	v1api := promv1.NewAPI(client)
	ctx, cancel := context.WithTimeout(context.Background(), actualParams.PromTimeout)
	defer cancel()

	result, warnings, err := v1api.Query(ctx, query, actualParams.QueryTime)
	checkErr(err)

	if len(warnings) > 0 {
		printDebug("Prometheus warnings: %v\n", warnings)
	}

	vectorResult, isVector := result.(model.Vector)
	if isVector {
		for _, currentResult := range vectorResult {
			response = append(response, float64(currentResult.Value))
		}
	} else {
		// panic("Cannot get response from Prometheus for the following query:\n" + query)
		printDebug("Cannot get response from Prometheus for the following query:\n%+v\n", query)
	}

	printDebug("Prom respose: %+v\n", response)
	return response
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

func checkVariadic(slice []string) string {
	var output string

	if len(slice) == 0 {
		output = ""
	} else {
		output = slice[0]
	}

	return output
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
