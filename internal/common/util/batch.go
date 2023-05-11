package util

import "math"

func Batch(elements []string, batchSize int) [][]string {
	total := len(elements)

	totalFullBatches := int(math.Floor(float64(total) / float64(batchSize)))
	lastBatchSize := total % batchSize
	totalBatches := totalFullBatches
	if lastBatchSize != 0 {
		totalBatches++
	}

	batches := make([][]string, totalBatches)

	for i := 0; i < totalFullBatches; i++ {
		batches[i] = elements[i*batchSize : (i+1)*batchSize]
	}

	if lastBatchSize != 0 {
		batches[totalFullBatches] = elements[totalFullBatches*batchSize : totalFullBatches*batchSize+lastBatchSize]
	}

	return batches
}
