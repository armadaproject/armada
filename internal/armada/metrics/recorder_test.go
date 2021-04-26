package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMin(t *testing.T) {
	metricsRecorder := recordMetrics([]float64{1, 1, 2, 5, 50, 12234234})
	result := metricsRecorder.GetMetrics()
	assert.Equal(t, float64(1), result.min)
}

func TestMax(t *testing.T) {
	metricsRecorder := recordMetrics([]float64{1, 1, 12234234, 7, 64345, 23423, 12234233})
	result := metricsRecorder.GetMetrics()
	assert.Equal(t, float64(12234234), result.max)
}

func TestSum(t *testing.T) {
	metricsRecorder := recordMetrics([]float64{1, 10, 100, 1000, 10000})
	result := metricsRecorder.GetMetrics()
	assert.Equal(t, float64(11111), result.sum)
}

func TestCount(t *testing.T) {
	metricsRecorder := recordMetrics([]float64{1, 1, 12234234, 7, 64345, 23423, 12234233})
	result := metricsRecorder.GetMetrics()
	assert.Equal(t, uint64(7), result.count)
}

func TestCalculateMedian_EvenValues(t *testing.T) {
	metricsRecorder := NewDefaultJobDurationMetricsRecorder()
	assert.Equal(t, float64(0), metricsRecorder.GetMetrics().GetMedian())

	metricsRecorder = recordMetrics([]float64{1, 1})
	assert.Equal(t, float64(1), metricsRecorder.GetMetrics().GetMedian())

	metricsRecorder = recordMetrics([]float64{1, 1, 5, 8, 10, 10})
	assert.Equal(t, 6.5, metricsRecorder.GetMetrics().GetMedian())

	metricsRecorder = recordMetrics([]float64{10, 1, 5, 10, 1, 8})
	assert.Equal(t, 6.5, metricsRecorder.GetMetrics().GetMedian())
}

func TestCalculateMedian_OddValues(t *testing.T) {
	metricsRecorder := recordMetrics([]float64{1})
	assert.Equal(t, float64(1), metricsRecorder.GetMetrics().GetMedian())

	metricsRecorder = recordMetrics([]float64{1, 1, 5, 8, 10})
	assert.Equal(t, float64(5), metricsRecorder.GetMetrics().GetMedian())

	metricsRecorder = recordMetrics([]float64{10, 1, 8, 1, 5})
	assert.Equal(t, float64(5), metricsRecorder.GetMetrics().GetMedian())
}

func TestCalculateMedian_Empty(t *testing.T) {
	metricsRecorder := recordMetrics([]float64{})
	result := metricsRecorder.GetMetrics()
	assert.Equal(t, float64(0), result.GetMedian())
}

func TestBuckets(t *testing.T) {
	expected := map[float64]uint64{
		float64(10):   uint64(2),
		float64(100):  uint64(4),
		float64(1000): uint64(6),
	}
	metricsRecorder := NewFloatMetricsRecorder(10, 100, 1000)
	for _, value := range []float64{1, 10, 50, 100, 500, 1000, 5000} {
		metricsRecorder.Record(value)
	}

	assert.Equal(t, expected, metricsRecorder.GetMetrics().GetBuckets())
}

func recordMetrics(testValues []float64) *FloatMetricsRecorder {
	metricsRecorder := NewDefaultJobDurationMetricsRecorder()
	for _, value := range testValues {
		metricsRecorder.Record(value)
	}
	return metricsRecorder
}
