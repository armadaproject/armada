package metrics

import armadaresource "github.com/armadaproject/armada/internal/common/resource"

type ResourceMetricsRecorder struct {
	metrics map[string]*FloatMetricsRecorder
}

func NewResourceMetricsRecorder() *ResourceMetricsRecorder {
	return &ResourceMetricsRecorder{
		metrics: make(map[string]*FloatMetricsRecorder, 10),
	}
}

func (d *ResourceMetricsRecorder) Record(value armadaresource.ComputeResourcesFloat) {
	for resourceType, value := range value {
		resourceMetricRecorder, ok := d.metrics[resourceType]
		if !ok {
			resourceMetricRecorder = NewFloatMetricsRecorder()
			d.metrics[resourceType] = resourceMetricRecorder
		}
		resourceMetricRecorder.Record(value)
	}
}

func (d *ResourceMetricsRecorder) GetMetrics() ResourceMetrics {
	result := make(map[string]*FloatMetrics, len(d.metrics))

	for resourceType, metrics := range d.metrics {
		result[resourceType] = metrics.GetMetrics()
	}

	return result
}

type ResourceMetrics map[string]*FloatMetrics
