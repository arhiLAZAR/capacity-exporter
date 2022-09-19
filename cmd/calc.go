package main

import "math"

// Calculate how many additional pods can the cluster handle, based on CPU and Memory occupied by all pod's dependencies
func calculateClusterCanHandlePods(allocatableCPU, allocatableMemory, fullChainCPU, fullChainMemory int64, podsAmount int) int64 {
	var clusterCanHandlePods, clusterCanHandleAdditionalPods int64

	if podsAmount == 0 {
		clusterCanHandleAdditionalPods = 0
	} else {
		fullChainCPUPerPod := fullChainCPU / int64(podsAmount)
		clusterCanHandlePodsCPU := allocatableCPU / fullChainCPUPerPod

		fullChainMemoryPerPod := fullChainMemory / int64(podsAmount)
		clusterCanHandlePodsMemory := allocatableMemory / fullChainMemoryPerPod

		printDebug("Cluster can handle pods (AT ALL) per CPU: %+v, per Memory: %+v\n", clusterCanHandlePodsCPU, clusterCanHandlePodsMemory)

		if clusterCanHandlePodsCPU <= clusterCanHandlePodsMemory {
			clusterCanHandlePods = clusterCanHandlePodsCPU
		} else {
			clusterCanHandlePods = clusterCanHandlePodsMemory
		}

		clusterCanHandleAdditionalPods = clusterCanHandlePods - int64(podsAmount)
	}

	return clusterCanHandleAdditionalPods
}

// Calculate resource summary of the namespace and its dependents (applying ingressMultiplier)
func calculateFullChainResources(config *configType, namespace string, cpu, mem map[string]int64, ingressMultipliers map[string]float64) (int64, int64) {
	var cpuSum, memSum int64

	printDebug("Main Namespace, CPU: %+v, Mem: %+v\n", cpu[namespace], mem[namespace])

	for _, currentNamespace := range config.Namespaces {
		if currentNamespace.Name == namespace {
			for _, dependantNamespace := range currentNamespace.DependsOnFullChain {
				printDebug("Dependant Namespace: %+v, CPU: %+v, Mem: %+v\n", dependantNamespace, cpu[dependantNamespace], mem[dependantNamespace])
				cpuSum += cpu[dependantNamespace]
				memSum += mem[dependantNamespace]
			}
		}
	}

	// We add only a percent of shared resources...
	multiplier, multiplierExists := ingressMultipliers[namespace]
	if multiplierExists {
		printDebug("Ingress Multiplier: %+v\n", multiplier)
		cpuSum = int64(float64(cpuSum) * multiplier)
		memSum = int64(float64(memSum) * multiplier)
	}

	// ...and 100% of frontend resource
	cpuSum += cpu[namespace]
	memSum += mem[namespace]

	return cpuSum, memSum
}

// Return the biggest of used and requested CPU and Mem
func calculateReallyOccupiedResources(usedCPU, usedMem, requestedCPU, requestedMem int64) (int64, int64) {
	var reallyOccupiedCPU, reallyOccupiedMem int64

	if usedCPU >= requestedCPU {
		reallyOccupiedCPU = usedCPU
	} else {
		reallyOccupiedCPU = requestedCPU
	}

	if usedMem >= requestedMem {
		reallyOccupiedMem = usedMem
	} else {
		reallyOccupiedMem = requestedMem
	}

	return reallyOccupiedCPU, reallyOccupiedMem
}

// Calculate ratio between every ingress' RPS and total RPS
func calculateIngressMultipliers(config *configType, adjustedRPS map[string]int64) map[string]float64 {
	var RPSSum int64
	ingressMultiplier := make(map[string]float64)

	for _, currentNamespace := range config.Namespaces {
		if currentNamespace.Frontend {
			RPSSum += adjustedRPS[currentNamespace.Name]
		}
	}

	printDebug("RPSSum: %+v\n", RPSSum)

	for _, currentNamespace := range config.Namespaces {
		if currentNamespace.Frontend {
			ingressMultiplier[currentNamespace.Name] = float64(adjustedRPS[currentNamespace.Name]) / float64(RPSSum)
		}
	}

	return ingressMultiplier
}

// Apply frontend_successful_percentage and frontend_to_shared_percentage multipliers from config.yaml
func adjustRPS(config *configType, targetNamespace string, rawRPS int64) int64 {
	multiplier := 1.0

	for _, currentNamespace := range config.Namespaces {
		if currentNamespace.Name == targetNamespace {
			if currentNamespace.Frontend && currentNamespace.FrontendSuccessfulPercentage != 0 {
				multiplier *= currentNamespace.FrontendSuccessfulPercentage / 100
			}

			if currentNamespace.Frontend && currentNamespace.Shared && currentNamespace.FrontendToSharedPercentage != 0 {
				multiplier *= currentNamespace.FrontendToSharedPercentage / 100
			}
		}
	}

	printDebug("Multiplier: %+v\n", multiplier)

	adjustedRPS := math.Round(float64(rawRPS) * multiplier)
	return int64(adjustedRPS)
}
