package eventingester

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestEventIngester_RedisMetricsSingleOwnerRegistration(t *testing.T) {
	registry := prometheus.NewRegistry()
	originalRegisterer := prometheus.DefaultRegisterer
	originalGatherer := prometheus.DefaultGatherer
	prometheus.DefaultRegisterer = registry
	prometheus.DefaultGatherer = registry
	t.Cleanup(func() {
		prometheus.DefaultRegisterer = originalRegisterer
		prometheus.DefaultGatherer = originalGatherer
	})

	collector := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "armada_event_redis_test_single_owner_registration",
		Help: "test collector for single owner registration",
	})

	require.NoError(t, registerCollector(collector, "redis usage metrics"))

	metricFamilies, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, metricFamilies, 1)
	require.Equal(t, "armada_event_redis_test_single_owner_registration", metricFamilies[0].GetName())
}

func TestEventIngester_RedisMetricsRepeatedInit_NoDuplicateRegister(t *testing.T) {
	registry := prometheus.NewRegistry()
	originalRegisterer := prometheus.DefaultRegisterer
	originalGatherer := prometheus.DefaultGatherer
	prometheus.DefaultRegisterer = registry
	prometheus.DefaultGatherer = registry
	t.Cleanup(func() {
		prometheus.DefaultRegisterer = originalRegisterer
		prometheus.DefaultGatherer = originalGatherer
	})

	firstCollector := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "armada_event_redis_test_repeated_init_registration",
		Help: "test collector for repeated registration",
	})
	secondCollector := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "armada_event_redis_test_repeated_init_registration",
		Help: "test collector for repeated registration",
	})

	require.NoError(t, registerCollector(firstCollector, "redis usage metrics"))
	require.NoError(t, registerCollector(secondCollector, "redis usage metrics"))

	metricFamilies, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, metricFamilies, 1)
	require.Equal(t, "armada_event_redis_test_repeated_init_registration", metricFamilies[0].GetName())
}
