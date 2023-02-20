package metrics

import (
	"testing"
	"time"

	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/stretchr/testify/assert"
)

func TestRecordJobRuntime_ShouldReportRuntimeDurations(t *testing.T) {
	jobMetricsRecorder := NewJobMetricsRecorder()
	jobMetricsRecorder.RecordJobRuntime("cpu", "", time.Second)
	jobMetricsRecorder.RecordJobRuntime("cpu", "", 2*time.Second)
	jobMetricsRecorder.RecordJobRuntime("cpu", "", 3*time.Second)

	jobMetricsRecorder.RecordJobRuntime("gpu", "", 2*time.Second)
	jobMetricsRecorder.RecordJobRuntime("gpu", "", 3*time.Second)

	jobMetricsRecorder.RecordJobRuntime("memory", "", 6*time.Second)
	jobMetricsRecorder.RecordJobRuntime("memory", "", 8*time.Second)

	jobMetricsRecorder.RecordJobRuntime("disk", "", 8*time.Second)
	jobMetricsRecorder.RecordJobRuntime("disk", "", 10*time.Second)
	for _, queueMetric := range jobMetricsRecorder.Metrics() {
		switch queueMetric.Pool {
		case "cpu":
			assert.Equal(t, float64(1), queueMetric.Durations.GetMin())
			assert.Equal(t, float64(3), queueMetric.Durations.GetMax())
			assert.Equal(t, float64(6), queueMetric.Durations.GetSum())
			assert.Equal(t, uint64(3), queueMetric.Durations.GetCount())
		case "gpu":
			assert.Equal(t, float64(2), queueMetric.Durations.GetMin())
			assert.Equal(t, float64(3), queueMetric.Durations.GetMax())
			assert.Equal(t, float64(5), queueMetric.Durations.GetSum())
			assert.Equal(t, uint64(2), queueMetric.Durations.GetCount())
		case "memory":
			assert.Equal(t, float64(6), queueMetric.Durations.GetMin())
			assert.Equal(t, float64(8), queueMetric.Durations.GetMax())
			assert.Equal(t, float64(14), queueMetric.Durations.GetSum())
			assert.Equal(t, uint64(2), queueMetric.Durations.GetCount())
		case "disk":
			assert.Equal(t, float64(8), queueMetric.Durations.GetMin())
			assert.Equal(t, float64(10), queueMetric.Durations.GetMax())
			assert.Equal(t, float64(18), queueMetric.Durations.GetSum())
			assert.Equal(t, uint64(2), queueMetric.Durations.GetCount())
		}
	}
}

func TestRecordResources_ShouldReportRuntimResources(t *testing.T) {
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

	for _, queueMetric := range jobMetricsRecorder.Metrics() {
		assert.Equal(t, float64(8), queueMetric.Resources["cpu"].GetMax())
		assert.Equal(t, float64(2), queueMetric.Resources["cpu"].GetMin())
		assert.Equal(t, float64(5), queueMetric.Resources["cpu"].GetMedian())

		assert.Equal(t, float64(5), queueMetric.Resources["gpu"].GetMax())
		assert.Equal(t, float64(2), queueMetric.Resources["gpu"].GetMin())
		assert.Equal(t, float64(3), queueMetric.Resources["gpu"].GetMedian())

		assert.Equal(t, float64(9), queueMetric.Resources["memory"].GetMax())
		assert.Equal(t, float64(3), queueMetric.Resources["memory"].GetMin())
		assert.Equal(t, float64(7), queueMetric.Resources["memory"].GetMedian())

		assert.Equal(t, float64(9), queueMetric.Resources["disk"].GetMax())
		assert.Equal(t, float64(5), queueMetric.Resources["disk"].GetMin())
		assert.Equal(t, float64(7), queueMetric.Resources["disk"].GetMedian())
	}
}
