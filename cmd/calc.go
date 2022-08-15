package main

import "math"

// Apply frontend_successful_percentage and frontend_to_shared_percentage multipliers from config.yaml
func adjustRPS(config *configType, targetNamespace string, rawRPS float64) float64 {
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
	return math.Round(rawRPS * multiplier)
}
