package metrics

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	armadaresource "github.com/armadaproject/armada/internal/common/resource"
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

func TestRecordResources_ShouldReportRuntimeResources(t *testing.T) {
	crf := armadaresource.ComputeResourcesFloat{
		"cpu":    float64(2),
		"memory": float64(3),
		"gpu":    float64(2),
		"disk":   float64(5),
	}
	crf2 := armadaresource.ComputeResourcesFloat{
		"cpu":    float64(8),
		"memory": float64(7),
		"gpu":    float64(5),
		"disk":   float64(7),
	}

	crf3 := armadaresource.ComputeResourcesFloat{
		"cpu":    float64(5),
		"memory": float64(9),
		"gpu":    float64(3),
		"disk":   float64(9),
	}

	jobMetricsRecorder := NewJobMetricsRecorder()
	jobMetricsRecorder.RecordResources("cpu", "", crf)
	jobMetricsRecorder.RecordResources("cpu", "", crf2)
	jobMetricsRecorder.RecordResources("cpu", "", crf3)
	expected := map[string][]float64{
		"cpu":    {8, 2, 5, 15},
		"gpu":    {5, 2, 3, 10},
		"memory": {9, 3, 7, 19},
		"disk":   {9, 5, 7, 14},
	}
	for _, queueMetric := range jobMetricsRecorder.Metrics() {
		assert.Equal(t, expected[queueMetric.Pool], []float64{
			queueMetric.Resources[queueMetric.Pool].GetMax(), queueMetric.Resources[queueMetric.Pool].GetMin(),
			queueMetric.Resources[queueMetric.Pool].GetMedian(), queueMetric.Resources[queueMetric.Pool].GetSum(),
		})
	}
}
