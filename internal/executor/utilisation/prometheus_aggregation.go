package utilisation

import (
	"github.com/prometheus/common/model"

	"github.com/armadaproject/armada/internal/executor/configuration"
)

func aggregateSamples(samples model.Vector, aggType configuration.AggregateType) float64 {
	switch aggType {
	case configuration.Mean:
		return meanSamples(samples)
	case configuration.Sum:
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
