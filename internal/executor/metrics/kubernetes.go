package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/component-base/metrics/legacyregistry"
	_ "k8s.io/component-base/metrics/prometheus/clientgo"
)

func GetMetricsGatherer() prometheus.Gatherer {
	// kubernetes component-base metric registry includes GO & Process collector, unregistering them in
	// DefaultRegisterer prevents error from duplicate metrics
	// Using just component-base does not currently work with promrus package
	// Unregister works even on different instance as collectors are tracked by id composed from descriptions
	prometheus.DefaultRegisterer.Unregister(prometheus.NewGoCollector())
	prometheus.DefaultRegisterer.Unregister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	return prometheus.Gatherers{legacyregistry.DefaultGatherer, prometheus.DefaultGatherer}
}
