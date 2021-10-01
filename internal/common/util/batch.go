package util

func Batch(elements []string, batchSize int) [][]string {
	var batch []string
	batchIdx := 0
	result := [][]string{}
	for i, e := range elements {
		if batch == nil || batchIdx >= len(batch) {
			batch = make([]string, Min(batchSize, len(elements)-i))
			result = append(result, batch)
			batchIdx = 0
		}

		batch[batchIdx] = e
		batchIdx++
	}
	return result
}
