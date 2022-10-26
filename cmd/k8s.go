package main

import (
	"context"
	"strings"

	appsV1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	metricsv "k8s.io/metrics/pkg/client/clientset/versioned"
)

// Count amount of used Cpu and Memory for specified namespace and deployment
func getUsedResources(namespace string, deploymentName ...string) (int64, int64) {
	var cpuSum, memSum int64
	clientset := getMetricsClientset()
	actualDeploymentName := checkVariadic(deploymentName)

	podMetricsList, err := clientset.MetricsV1beta1().PodMetricses(namespace).List(context.TODO(), metav1.ListOptions{})
	checkErr(err)

	for _, pod := range podMetricsList.Items {
		if strings.HasPrefix(pod.Name, actualDeploymentName) {
			for _, container := range pod.Containers {
				printWithTabs("Pod: "+pod.Name+"\tContainer: "+container.Name, 10)
				printDebug("UsedMilliCpu: %+v\t", container.Usage.Cpu().MilliValue())
				printDebug("UsedMem: %+v\n", container.Usage.Memory().Value())

				cpuSum += container.Usage.Cpu().MilliValue()
				memSum += container.Usage.Memory().Value()
			}
		}
	}

	return cpuSum, memSum
}

// Get total amount of requested memory and cpu on specified nodes
func getTotalRequestedResources(nodes []string, pods *v1.PodList) (int64, int64) {
	var cpuSum, memSum int64

	for _, pod := range pods.Items {
		if inList(pod.Spec.NodeName, nodes) {
			for _, container := range pod.Spec.Containers {
				cpuSum += container.Resources.Requests.Cpu().MilliValue()
				memSum += container.Resources.Requests.Memory().Value()
			}
		}
	}

	return cpuSum, memSum
}

// Get amount of requested memory and cpu for specified deployment
func getDeploymentRequestedResources(namespace, deploymentName string) (int64, int64) {
	var cpuSum, memSum, replicaCount int64
	deploymentList := getDeploymentList(namespace)

	if len(deploymentList.Items) > 0 {
		for _, deployment := range deploymentList.Items {
			if deployment.Name == deploymentName {

				var containerCPU, containerMem int64
				replicaCount = int64(*deployment.Spec.Replicas)
				containers := deployment.Spec.Template.Spec.Containers

				if len(containers) > 0 {
					for _, container := range containers {
						containerCPU += container.Resources.Requests.Cpu().MilliValue()
						containerMem += container.Resources.Requests.Memory().Value()
					}
				}
				cpuSum += containerCPU * replicaCount
				memSum += containerMem * replicaCount

			}
		}
	}

	return cpuSum, memSum
}

// Get total amount of allocatable memory and cpu for nodes with relevant labels in the specific namespace
// If allowed labels are specified then count the node only if the labels match
// If forbidden labels are specified then count the node only if the labels do not match
func getAllocatableResources(namespace, deploymentName string, deploymentLabels deploymentLabelsType, nodeList *v1.NodeList) (int64, int64, []string) {
	var everythingAllowed, nothingForbidden, thisNodeIsAllowed, thisNodeIsForbidden bool
	var cpuSum, memSum int64
	var allowedNodes []string

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
		if everythingAllowed && nothingForbidden {
			printDebug("All nodes are allowed, none are forbidden. ")
			thisNodeIsAllowed = true
		} else {

			if !everythingAllowed && !nothingForbidden {

				thisNodeIsForbidden = labelsAreEqual(node.Labels, deploymentLabels.Forbidden, "both-forbidden")
				if !thisNodeIsForbidden {
					thisNodeIsAllowed = labelsAreEqual(node.Labels, deploymentLabels.Allowed, "both-allowed")
				}

			} else {

				if !everythingAllowed {
					thisNodeIsAllowed = labelsAreEqual(node.Labels, deploymentLabels.Allowed, "allowed")
				}
				if !nothingForbidden {
					thisNodeIsAllowed = !labelsAreEqual(node.Labels, deploymentLabels.Forbidden, "forbidden")
				}

			}

		}

		if thisNodeIsAllowed {
			printDebug("Node \"%+v\" is allowed ", node.Name)

			if !nodeIsTainted(namespace, deploymentName, node.Spec.Taints) {
				printDebug("and not tainted!\n")

				cpuSum += node.Status.Capacity.Cpu().MilliValue()
				memSum += node.Status.Capacity.Memory().Value()
				allowedNodes = append(allowedNodes, node.Name)

			} else {
				printDebug("BUT tainted!\n")
			}
		}

	}

	return cpuSum, memSum, allowedNodes
}

func labelsAreEqual(nodeLabels map[string]string, deploymentLabels []allowedAndForbiddenLabelsType, checkType ...string) bool {
	labelsAreEqual := false

	for nodeLabelKey, nodeLabelValue := range nodeLabels {
		printDebug("%s check %s", printWithTabs("Nodelabel: "+nodeLabelKey, 6, false), checkType[0])
		for _, deploymentLabel := range deploymentLabels {
			// Do not count this node if the node and the deployment has the same label...
			if nodeLabelKey == deploymentLabel.Key {
				for _, deploymentLabelValue := range deploymentLabel.Values {
					// ...with the same value
					if nodeLabelValue == deploymentLabelValue {
						labelsAreEqual = true
					}
				}
			}
		}
		printDebug("\t%v\n", labelsAreEqual)
	}

	return labelsAreEqual
}

// Check if the deployment tolerates to all node's taints
func nodeIsTainted(namespace, deploymentName string, nodeTaints []v1.Taint) bool {
	nodeIsTainted := false

	if len(nodeTaints) > 0 {
		deploymentList := getDeploymentList(namespace)
		if len(deploymentList.Items) > 0 {
			for _, taint := range nodeTaints {
				nodeIsTainted = true

				for _, deployment := range deploymentList.Items {
					if deployment.Name == deploymentName {

						for _, toleration := range deployment.Spec.Template.Spec.Tolerations {
							if toleration.Key == taint.Key {
								if toleration.Operator == "Exists" || (toleration.Operator == "Equal" && toleration.Value == taint.Value) {
									nodeIsTainted = false
								}
							}
						}

						if nodeIsTainted {
							return nodeIsTainted
						}

					}
				}

			}
		}
	}

	return nodeIsTainted
}

// Check if the deployment in the specified namespace has some affinities
func getAntiAffinityLabels(config *configType, namespace, deploymentName string) deploymentLabelsType {
	var deploymentLabels deploymentLabelsType
	deploymentList := getDeploymentList(namespace)

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
									var labels allowedAndForbiddenLabelsType
									labels.Key = deploymentAffinity.Key
									labels.Values = deploymentAffinity.Values

									deploymentLabels.Allowed = append(deploymentLabels.Allowed, labels)
								}
								if deploymentAffinity.Operator == "NotIn" {
									var labels allowedAndForbiddenLabelsType
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

// Get a list of all nodes in the cluster
func getNodeList() v1.NodeList {
	clientset := getMetaV1Clientset()

	nodeList, err := clientset.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
	checkErr(err)

	return *nodeList
}

// Get a list of all or namespaced pods in the cluster
// Deployment Name may be specified or not
func getPodList(params ...string) v1.PodList {
	clientset := getMetaV1Clientset()
	namespace := checkVariadic(params, 0)
	deploymentName := checkVariadic(params, 1)

	podList, err := clientset.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{})
	checkErr(err)

	// Return only pods with the correct Deployment Name
	if deploymentName != "" {
		var tmpPodList []v1.Pod

		for _, pod := range podList.Items {
			if strings.HasPrefix(pod.Name, deploymentName) {
				tmpPodList = append(tmpPodList, pod)
			}
		}

		podList.Items = tmpPodList
	}

	return *podList
}

func getDeploymentList(namespace ...string) appsV1.DeploymentList {
	clientset := getMetaV1Clientset()
	actualNamespace := checkVariadic(namespace)

	deploymentList, err := clientset.AppsV1().Deployments(actualNamespace).List(context.TODO(), metav1.ListOptions{})
	checkErr(err)

	return *deploymentList
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
