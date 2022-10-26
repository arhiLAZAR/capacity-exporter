package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"time"

	promapi "github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/spf13/pflag"
	"gopkg.in/yaml.v3"
	v1 "k8s.io/api/core/v1"
)

const (
	defaultConfigPath             = "/app/config.yaml"
	DEBUG                         = true // TODO: read DEBUG var from ENV
	prometheusDefaultTimeout      = 10
	exporterNamespace             = "capacity"
	exporterDefaultPort           = 9301
	exporterDefaultScrapeInterval = 10
)

type configType struct {
	Prometheus struct {
		Address       string
		Timeout       int64
		QueryTemplate string `yaml:"query_template"`
	}

	Exporter struct {
		Host            string
		Port            int64
		MetricsEndpoint string `yaml:"metrics_endpoint"`
	}

	Affinity []struct {
		Key      string
		Value    string
		Operator v1.NodeSelectorOperator
	}

	AllDeploymentsPrefix string `yaml:"all_deployments_prefix"`
	AllDeploymentsSuffix string `yaml:"all_deployments_suffix"`

	Namespaces []struct {
		Name                         string
		Frontend                     bool
		FrontendSuccessfulPercentage float64 `yaml:"frontend_successful_percentage"`
		Shared                       bool
		FrontendToSharedPercentage   float64  `yaml:"frontend_to_shared_percentage"`
		DeploymentAlias              string   `yaml:"deployment_alias"`
		DeploymentPrefix             string   `yaml:"deployment_prefix"`
		DeploymentSuffix             string   `yaml:"deployment_suffix"`
		DependsOn                    []string `yaml:"depends_on"`
		DependsOnFullChain           []string
		Prometheus                   struct {
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
	var allowedNodes []string

	allocatableCPU := make(map[string]int64)
	allocatableMemory := make(map[string]int64)
	totalRequestedCPU := make(map[string]int64)
	totalRequestedMemory := make(map[string]int64)
	deploymentRequestedCPU := make(map[string]int64)
	deploymentRequestedMemory := make(map[string]int64)
	usedCPU := make(map[string]int64)
	usedMemory := make(map[string]int64)
	reallyOccupiedCPU := make(map[string]int64)
	reallyOccupiedMemory := make(map[string]int64)
	fullChainCPU := make(map[string]int64)
	fullChainMemory := make(map[string]int64)
	rawRPS := make(map[string]int64)
	adjustedRPS := make(map[string]int64)
	podsAmount := make(map[string]int)
	clusterCanHandleAdditionalPods := make(map[string]int64)
	oneRPSCostCPU := make(map[string]float64)
	oneRPSCostMemory := make(map[string]float64)

	metricRPSCostCPU := make(map[string]prometheus.Gauge)
	metricRPSCostMemory := make(map[string]prometheus.Gauge)
	metricPodAmount := make(map[string]prometheus.Gauge)
	metricClusterCanHandleAdditionalPods := make(map[string]prometheus.Gauge)
	metricRawRPS := make(map[string]prometheus.Gauge)
	metricAdjustedRPS := make(map[string]prometheus.Gauge)

	config := readConfig()

	// Create Prometheus metrics
	for _, app := range getAllNamespaces(&config) {
		labels := map[string]string{"app": app}

		metricRPSCostCPU[app] = createGauge("rps_cost_cpu", "How many milliCPUs costs one RPS", labels)
		metricRPSCostMemory[app] = createGauge("rps_cost_mem", "How many Memory bytes costs one RPS", labels)
		metricPodAmount[app] = createGauge("pod_amount", "Current amount of pods", labels)
		metricClusterCanHandleAdditionalPods[app] = createGauge("cluster_can_handle_additional_pods", "How many additional pods can the current cluster handle", labels)
		metricRawRPS[app] = createGauge("rps_raw", "Raw RPS from Prometheus", labels)
		metricAdjustedRPS[app] = createGauge("rps_adjusted", "Adjusted RPS with multipliers from config", labels)
	}

	go func() {
		for {
			nodeList := getNodeList()
			podList := getPodList()

			for nsNum, namespace := range config.Namespaces {
				nsName := namespace.Name

				deploymentName := getDeploymentName(&config, nsName)
				deploymentLabels := getAntiAffinityLabels(&config, nsName, deploymentName)
				printDebug("Namespace: \"%s\"\nAllowed labels: %+v\nForbidden labels: %+v\n", nsName, deploymentLabels.Allowed, deploymentLabels.Forbidden)

				allocatableCPU[nsName], allocatableMemory[nsName], allowedNodes = getAllocatableResources(deploymentLabels, &nodeList)
				printDebug("Allocatable MilliCpuSum: %+v\nAllocatable MemSum: %+v\nAllowed nodes: %+v\n", allocatableCPU[nsName], allocatableMemory[nsName], allowedNodes)

				totalRequestedCPU[nsName], totalRequestedMemory[nsName] = getTotalRequestedResources(allowedNodes, &podList)
				printDebug("Total Requested MilliCpuSum: %+v\nTotal Requested MemSum: %+v\n", totalRequestedCPU[nsName], totalRequestedMemory[nsName])

				deploymentRequestedCPU[nsName], deploymentRequestedMemory[nsName] = getDeploymentRequestedResources(nsName, deploymentName)
				printDebug("Deployment Requested MilliCpuSum: %+v\nDeployment Requested MemSum: %+v\n", deploymentRequestedCPU[nsName], deploymentRequestedMemory[nsName])

				podsAmount[nsName] = len(getPodList(nsName, deploymentName).Items)
				printDebug("Amount of pods: %+v\n", podsAmount[nsName])

				usedCPU[nsName], usedMemory[nsName] = getUsedResources(nsName, deploymentName)
				printDebug("Used MilliCpuSum: %+v\nUsed MemSum: %+v\n", usedCPU[nsName], usedMemory[nsName])

				reallyOccupiedCPU[nsName], reallyOccupiedMemory[nsName] = calculateReallyOccupiedResources(usedCPU[nsName], usedMemory[nsName], deploymentRequestedCPU[nsName], deploymentRequestedMemory[nsName])
				printDebug("Really Occupied MilliCpuSum: %+v\nReally Occupied MemSum: %+v\n", reallyOccupiedCPU[nsName], reallyOccupiedMemory[nsName])

				config.Namespaces[nsNum].DependsOnFullChain = getDependencies(&config, nsName)
				printDebug("Dependencies: %+v\n", config.Namespaces[nsNum].DependsOnFullChain)

				rawRPS[nsName] = getRPS(&config, nsName)
				printDebug("Raw RPS: %+v\n", rawRPS[nsName])

				adjustedRPS[nsName] = adjustRPS(&config, nsName, rawRPS[nsName])
				printDebug("Adjusted RPS: %+v\n", adjustedRPS[nsName])

				printDebug("\n")
			}

			printDebug("\n###### FINAL CALCULATIONS! ######\n\n")
			ingressMultipliers := calculateIngressMultipliers(&config, adjustedRPS)
			printDebug("Ingress multipliers: %+v\n\n", ingressMultipliers)

			for _, namespace := range config.Namespaces {
				nsName := namespace.Name
				printDebug("Namespace: \"%s\"\n", nsName)

				fullChainCPU[nsName], fullChainMemory[nsName] = calculateFullChainResources(&config, nsName, reallyOccupiedCPU, reallyOccupiedMemory, ingressMultipliers)
				printDebug("Full Chain MilliCpuSum: %+v\nFull Chain MemSum: %+v\n", fullChainCPU[nsName], fullChainMemory[nsName])

				clusterCanHandleAdditionalPods[nsName] = calculateClusterCanHandlePods(allocatableCPU[nsName], allocatableMemory[nsName], fullChainCPU[nsName], fullChainMemory[nsName], podsAmount[nsName])
				printDebug("Cluster can handle %+v additional pods\n", clusterCanHandleAdditionalPods[nsName])

				oneRPSCostCPU[nsName], oneRPSCostMemory[nsName] = calculateOneRPSCost(fullChainCPU[nsName], fullChainMemory[nsName], adjustedRPS[nsName])
				printDebug("One RPS costs: %+v MilliCPU, %+v Memory (bytes)\n", oneRPSCostCPU[nsName], oneRPSCostMemory[nsName])

				printDebug("\n")

				// Set Prometheus metrics
				metricRPSCostCPU[nsName].Set(oneRPSCostCPU[nsName])
				metricRPSCostMemory[nsName].Set(oneRPSCostMemory[nsName])
				metricPodAmount[nsName].Set(float64(podsAmount[nsName]))
				metricClusterCanHandleAdditionalPods[nsName].Set(float64(clusterCanHandleAdditionalPods[nsName]))
				metricRawRPS[nsName].Set(float64(rawRPS[nsName]))
				metricAdjustedRPS[nsName].Set(float64(adjustedRPS[nsName]))
			}

			// TODO: read exporterScrapeInterval from config
			time.Sleep(exporterDefaultScrapeInterval * time.Second)

		}
	}()

	serveExporter(&config)

}

// Get Requests Per Second for the specified namespace (from Prometheus)
func getRPS(config *configType, namespace string) int64 {

	promAddress := config.Prometheus.Address
	promQuery := parsePromQuery(config, namespace)

	promResponse := promRequest(promAddress, promQuery)

	if len(promResponse) == 0 {
		return 0
	}
	return int64(promResponse[0])
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

	printDebug("Prom response: %+v\n", response)
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

func checkVariadic(slice []string, elementNum ...int64) string {
	var output string
	var actualElementNum int64

	if len(elementNum) == 0 {
		actualElementNum = 0
	} else {
		actualElementNum = elementNum[0]
	}

	if len(slice) == 0 {
		output = ""
	} else {
		output = slice[actualElementNum]
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
