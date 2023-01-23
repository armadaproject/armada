package utilisation

import (
	"github.com/prometheus/common/model"
)

type AggregateType int64

const (
	Sum AggregateType = iota
	Mean
)

func aggregateSamples(samples model.Vector, aggType AggregateType) float64 {
	switch aggType {
	case Mean:
		return meanSamples(samples)
	case Sum:
		return sumSamples(samples)
	default:
		return 0.0
	}
}

func sumSamples(samples model.Vector) float64 {
	var result float64
	for _, sample := range samples {
		result += float64(sample.Value)
	}
	return result
}

func meanSamples(samples model.Vector) float64 {
	return sumSamples(samples) / float64(len(samples))
}
