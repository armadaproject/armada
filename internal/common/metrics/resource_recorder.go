package metrics

import (
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/pkg/bidstore"
)

type ResourceMetricsRecorder struct {
	metrics map[bidstore.PriceBand]map[string]*FloatMetricsRecorder
}

func NewResourceMetricsRecorder() *ResourceMetricsRecorder {
	return &ResourceMetricsRecorder{
		metrics: make(map[bidstore.PriceBand]map[string]*FloatMetricsRecorder, 10),
	}
}

func (d *ResourceMetricsRecorder) Record(priceBand bidstore.PriceBand, value armadaresource.ComputeResourcesFloat) {
	if _, ok := d.metrics[priceBand]; !ok {
		d.metrics[priceBand] = map[string]*FloatMetricsRecorder{}
	}
	for resourceType, value := range value {
		resourceMetricRecorder, ok := d.metrics[priceBand][resourceType]
		if !ok {
			resourceMetricRecorder = NewFloatMetricsRecorder()
			d.metrics[priceBand][resourceType] = resourceMetricRecorder
		}
		resourceMetricRecorder.Record(value)
	}
}

type ResourceMetrics map[bidstore.PriceBand]map[string]*FloatMetrics

func (d *ResourceMetricsRecorder) GetMetrics() ResourceMetrics {
	result := make(map[bidstore.PriceBand]map[string]*FloatMetrics, len(d.metrics))
	for priceBand, resources := range d.metrics {
		if _, ok := result[priceBand]; !ok {
			result[priceBand] = map[string]*FloatMetrics{}
		}
		for resourceType, metrics := range resources {
			result[priceBand][resourceType] = metrics.GetMetrics()
		}
	}
	return result
}
