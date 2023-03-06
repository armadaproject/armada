package metrics

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	armadaresource "github.com/armadaproject/armada/internal/common/resource"
)

var (
	crf1 = armadaresource.ComputeResourcesFloat{
		"cpu":    float64(2),
		"memory": float64(3),
		"gpu":    float64(2),
		"disk":   float64(5),
	}
	crf2 = armadaresource.ComputeResourcesFloat{
		"cpu":    float64(8),
		"memory": float64(7),
		"gpu":    float64(5),
		"disk":   float64(7),
	}

	crf3 = armadaresource.ComputeResourcesFloat{
		"cpu":    float64(5),
		"memory": float64(9),
		"gpu":    float64(3),
		"disk":   float64(9),
	}

	crf4 = armadaresource.ComputeResourcesFloat{
		"cpu":    float64(1),
		"memory": float64(14),
		"gpu":    float64(2),
		"disk":   float64(20),
	}
)

func TestRecordJobRuntime_ShouldReportRuntimeDurations(t *testing.T) {
	jobMetricsRecorder := NewJobMetricsRecorder()
	jobMetricsRecorder.RecordJobRuntime("pool-1", "", time.Second)
	jobMetricsRecorder.RecordJobRuntime("pool-1", "", 2*time.Second)
	jobMetricsRecorder.RecordJobRuntime("pool-1", "", 3*time.Second)

	jobMetricsRecorder.RecordJobRuntime("pool-2", "", 2*time.Second)
	jobMetricsRecorder.RecordJobRuntime("pool-2", "", 3*time.Second)

	jobMetricsRecorder.RecordJobRuntime("pool-3", "", 6*time.Second)
	jobMetricsRecorder.RecordJobRuntime("pool-3", "", 8*time.Second)

	jobMetricsRecorder.RecordJobRuntime("pool-4", "", 8*time.Second)
	jobMetricsRecorder.RecordJobRuntime("pool-4", "", 10*time.Second)
	expected := map[string][]float64{
		"pool-1": {1, 3, 6},
		"pool-2": {2, 3, 5},
		"pool-3": {6, 8, 14},
		"pool-4": {8, 10, 18},
	}
	for _, queueMetric := range jobMetricsRecorder.Metrics() {
		assert.Equal(t, expected[queueMetric.Pool], []float64{queueMetric.Durations.GetMin(), queueMetric.Durations.GetMax(), queueMetric.Durations.GetSum()})
	}
}

func TestRecordResources_ShouldReportRuntimeResourcesForSinglePool(t *testing.T) {
	jobMetricsRecorder := NewJobMetricsRecorder()
	jobMetricsRecorder.RecordResources("pool-1", "", crf1)
	jobMetricsRecorder.RecordResources("pool-1", "", crf2)
	jobMetricsRecorder.RecordResources("pool-1", "", crf3)
	expected := map[string][]float64{
		"cpu":    {8, 2, 5, 15},
		"gpu":    {5, 2, 3, 10},
		"memory": {9, 3, 7, 19},
		"disk":   {9, 5, 7, 21},
	}
	for _, queueMetric := range jobMetricsRecorder.Metrics() {
		for resource, expectedValues := range expected {
			assert.Equal(t, expectedValues, []float64{
				queueMetric.Resources[resource].GetMax(), queueMetric.Resources[resource].GetMin(),
				queueMetric.Resources[resource].GetMedian(), queueMetric.Resources[resource].GetSum(),
			})
		}
	}
}

func TestRecordResources_ShouldReportRuntimeResourcesForMultiplePools(t *testing.T) {
	jobMetricsRecorder := NewJobMetricsRecorder()
	jobMetricsRecorder.RecordResources("pool-1", "", crf1)
	jobMetricsRecorder.RecordResources("pool-1", "", crf2)
	jobMetricsRecorder.RecordResources("pool-2", "", crf3)
	jobMetricsRecorder.RecordResources("pool-2", "", crf4)

	expected := map[string]map[string][]float64{
		"pool-1": {
			"cpu":    {8, 2, 5, 10},
			"gpu":    {5, 2, 3.5, 7},
			"memory": {7, 3, 5, 10},
			"disk":   {7, 5, 6, 12},
		},
		"pool-2": {
			"cpu":    {5, 1, 3, 6},
			"gpu":    {3, 2, 2.5, 5},
			"memory": {14, 9, 11.5, 23},
			"disk":   {20, 9, 14.5, 29},
		},
	}

	for _, queueMetric := range jobMetricsRecorder.Metrics() {
		for resource, expectedValues := range expected[queueMetric.Pool] {
			assert.Equal(t, expectedValues, []float64{
				queueMetric.Resources[resource].GetMax(), queueMetric.Resources[resource].GetMin(),
				queueMetric.Resources[resource].GetMedian(), queueMetric.Resources[resource].GetSum(),
			})
		}
	}
}
