package utilisation

import (
	"testing"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"

	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/executor/configuration"
	"github.com/armadaproject/armada/internal/executor/domain"
)

func TestToQuantity(t *testing.T) {
	assert.Equal(t, makeQuantity(0), toQuantity(0))
	assert.Equal(t, makeQuantity(1), toQuantity(1))
	assert.Equal(t, makeMilliQuantity(1500), toQuantity(1.5))
	assert.Equal(t, makeMilliQuantity(333), toQuantity(1.0/3.0))
	q1 := toQuantity(1)
	assert.Equal(t, "1", q1.String())
	q2 := toQuantity(1.5)
	assert.Equal(t, "1500m", q2.String())
}

func TestExtractPrometheusMetricNames(t *testing.T) {
	config := makeTestConfig()

	result := extractPrometheusMetricNames(config)

	assert.Equal(t, []string{"DCGM_FI_DEV_GPU_UTIL", "DCGM_FI_DEV_MEM_COPY_UTIL"}, result)
}

func TestUpdateMetrics(t *testing.T) {
	config := makeTestConfig()
	samples := makeTestSamples()
	podNameToUtilisationData := map[string]*domain.UtilisationData{
		"pod1": domain.EmptyUtilisationData(),
		"pod2": domain.EmptyUtilisationData(),
		"pod3": domain.EmptyUtilisationData(),
	}

	updateMetrics(samples, config, podNameToUtilisationData)

	expectedUtilisationData := map[string]*domain.UtilisationData{
		"pod1": {
			CurrentUsage: armadaresource.ComputeResources{
				"gpu":                          makeQuantity(1),
				"accelerator-memory-copy-util": makeQuantity(4),
			},
			CumulativeUsage: armadaresource.ComputeResources{},
		},
		"pod2": {
			CurrentUsage: armadaresource.ComputeResources{
				"gpu":                          makeMilliQuantity(550),
				"accelerator-memory-copy-util": makeQuantity(5),
			},
			CumulativeUsage: armadaresource.ComputeResources{},
		},
		"pod3": domain.EmptyUtilisationData(),
	}

	assert.Equal(t, expectedUtilisationData, podNameToUtilisationData)
}

func makeTestConfig() []configuration.CustomUsageMetric {
	return []configuration.CustomUsageMetric{
		{
			Name:                   "gpu",
			PrometheusMetricName:   "DCGM_FI_DEV_GPU_UTIL",
			PrometheusPodNameLabel: "pod",
			AggregateType:          configuration.Sum,
			Multiplier:             0.01,
		},
		{
			Name:                   "accelerator-memory-copy-util",
			PrometheusMetricName:   "DCGM_FI_DEV_MEM_COPY_UTIL",
			PrometheusPodNameLabel: "pod",
			AggregateType:          configuration.Mean,
		},
	}
}

func makeTestSamples() model.Vector {
	return []*model.Sample{
		{Metric: model.Metric{model.MetricNameLabel: "DCGM_FI_DEV_GPU_UTIL", "pod": "pod1", "gpu": "gpu1"}, Value: 100},
		{Metric: model.Metric{model.MetricNameLabel: "DCGM_FI_DEV_GPU_UTIL", "pod": "pod2", "gpu": "gpu2"}, Value: 20},
		{Metric: model.Metric{model.MetricNameLabel: "DCGM_FI_DEV_GPU_UTIL", "pod": "pod2", "gpu": "gpu3"}, Value: 35},
		{Metric: model.Metric{model.MetricNameLabel: "DCGM_FI_DEV_MEM_COPY_UTIL", "pod": "pod1", "gpu": "gpu1"}, Value: 4},
		{Metric: model.Metric{model.MetricNameLabel: "DCGM_FI_DEV_MEM_COPY_UTIL", "pod": "pod2", "gpu": "gpu2"}, Value: 4},
		{Metric: model.Metric{model.MetricNameLabel: "DCGM_FI_DEV_MEM_COPY_UTIL", "pod": "pod2", "gpu": "gpu3"}, Value: 6},
	}
}

func makeQuantity(val int64) resource.Quantity {
	return *resource.NewQuantity(val, resource.DecimalSI)
}

func makeMilliQuantity(milliVal int64) resource.Quantity {
	return *resource.NewMilliQuantity(milliVal, resource.DecimalSI)
}
