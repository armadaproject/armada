package eventlogger

import (
	"math"
)

type Statistics struct {
	Min               int64   `json:"min"`
	Max               int64   `json:"max"`
	Average           float64 `json:"average"`
	Variance          float64 `json:"variance"`
	StandardDeviation float64 `json:"standardDeviation"`
}

func statistics(durations []*EventDuration) *Statistics {
	durationsInt64 := extractDuration(durations)
	return &Statistics{
		Min:               minInt64(durationsInt64),
		Max:               maxInt64(durationsInt64),
		Average:           avgInt64(durationsInt64),
		Variance:          varianceInt64(durationsInt64),
		StandardDeviation: standardDeviationInt64(durationsInt64),
	}
}

func extractDuration(input []*EventDuration) []int64 {
	output := make([]int64, 0, len(input))
	for _, e := range input {
		output = append(output, int64(e.Duration))
	}
	return output
}

func minInt64(input []int64) int64 {
	var m int64
	for i, e := range input {
		if i == 0 || e < m {
			m = e
		}
	}
	return m
}

func maxInt64(input []int64) int64 {
	var m int64
	for i, e := range input {
		if i == 0 || e > m {
			m = e
		}
	}
	return m
}

func sumInt64(input []int64) int64 {
	var sum int64
	for _, e := range input {
		sum += e
	}
	return sum
}

func avgInt64(input []int64) float64 {
	num := len(input)
	if num == 0 {
		return 0
	}
	sum := sumInt64(input)
	avg := float64(sum) / float64(num)
	return avg
}

func varianceInt64(numbers []int64) float64 {
	if len(numbers) < 2 {
		return 0
	}
	var total float64
	avg := avgInt64(numbers)
	for _, number := range numbers {
		total += math.Pow(float64(number)-avg, 2)
	}
	num := len(numbers)
	variance := total / float64(num-1)
	return variance
}

func standardDeviationInt64(numbers []int64) float64 {
	variance := varianceInt64(numbers)
	stdDev := math.Sqrt(variance)
	return stdDev
}
