package utilisation

import (
	"math"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/executor/configuration"
)

func makeSamples(values ...float64) model.Vector {
	result := model.Vector{}
	for _, val := range values {
		result = append(result, &model.Sample{Value: model.SampleValue(val)})
	}
	return result
}

func TestAggregateSamples(t *testing.T) {
	samples := makeSamples(1, 1)
	assert.Equal(t, 2.0, aggregateSamples(samples, configuration.Sum))
	assert.Equal(t, 1.0, aggregateSamples(samples, configuration.Mean))
	assert.Equal(t, 0.0, aggregateSamples(samples, configuration.AggregateType(999)))
}

func TestSumSamples(t *testing.T) {
	assert.Equal(t, 0.0, sumSamples(makeSamples()))
	assert.Equal(t, 2.0, sumSamples(makeSamples(2)))
	assert.Equal(t, 3.0, sumSamples(makeSamples(2, 1)))
}

func TestMeanSamples(t *testing.T) {
	assert.True(t, math.IsNaN(meanSamples(makeSamples())))
	assert.Equal(t, 2.0, meanSamples(makeSamples(2)))
	assert.Equal(t, 1.5, meanSamples(makeSamples(2, 1)))
}
