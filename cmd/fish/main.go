package main

import (
	"fmt"
)

func calculateAdjustedShares(weights, desiredShares []float64, maxIterations int) []float64 {
	adjustedShares := make([]float64, len(weights))
	var reallocationFactor = 1.0
	for i := 0; i < maxIterations; i++ {
		var totalWeight = 0.0
		for _, w := range weights {
			totalWeight += w
		}

		for j := range weights {
			if weights[j] > 0 {
				share := (weights[j] / totalWeight) * reallocationFactor
				adjustedShares[j] += share
			}
		}
		unallocated := 0.0
		needsRecalc := false
		for j := range weights {
			excessShare := adjustedShares[j] - desiredShares[j]
			if excessShare > 0 {
				adjustedShares[j] = desiredShares[j]
				weights[j] = 0.0
				unallocated += excessShare
			} else {
				needsRecalc = true
			}
		}
		reallocationFactor = unallocated
		if !needsRecalc {
			break
		}
	}
	return adjustedShares
}

func main() {
	weights := []float64{2.0, 1.0, 1.0, 1.0}
	demand := []float64{0.5, 0.3, 0.001, 0.001}

	adjustedShares := calculateAdjustedShares(weights, demand, 5)

	sum := 0.0
	for i, share := range adjustedShares {
		sum += share
		fmt.Printf("User %d adjusted share: %.4f\n", i, share)
	}
	fmt.Printf("Total share: %.4f\n", sum)
}
