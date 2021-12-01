package util

import "math"

func Batch(elements []string, batchSize int) [][]string {
	total := len(elements)

	n := int(math.Floor(float64(total) / float64(batchSize)))
	lastBatchSize := total % batchSize
	totalBatches := n
	if lastBatchSize != 0 {
		totalBatches++
	}

	batches := make([][]string, totalBatches, totalBatches)

	for i := 0; i < n; i++ {
		batches[i] = elements[i*batchSize : (i+1)*batchSize]
	}

	if lastBatchSize != 0 {
		batches[n] = elements[n*batchSize : n*batchSize+lastBatchSize]
	}

	return batches
}
