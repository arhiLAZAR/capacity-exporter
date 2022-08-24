package main

import "math"

// Return the bigger of used and requested CPU and Mem
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
func calculateIngressMultiplier(config *configType, adjustedRPS map[string]int64) map[string]float64 {
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
