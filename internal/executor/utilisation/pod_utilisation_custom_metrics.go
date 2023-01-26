package utilisation

import (
	"math"
	"net/http"

	"github.com/prometheus/common/model"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/armadaproject/armada/internal/executor/configuration"
	clusterContext "github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/util"
)

type podUtilisationCustomMetrics struct {
	httpClient *http.Client
	config     *configuration.CustomUsageMetrics
	clock      clock.Clock
}

func newPodUtilisationCustomMetrics(httpClient *http.Client, config *configuration.CustomUsageMetrics) *podUtilisationCustomMetrics {
	log.Infof("Configuring %d custom usage metrics to be scraped from %s %s=%s", len(config.Metrics), config.Namespace, config.EndpointSelectorLabelName, config.EndpointSelectorLabelValue)
	return &podUtilisationCustomMetrics{httpClient: httpClient, config: config, clock: clock.RealClock{}}
}

func (m *podUtilisationCustomMetrics) fetch(nodes []*v1.Node, podNameToUtilisationData map[string]*domain.UtilisationData, clusterContext clusterContext.ClusterContext) {
	start := m.clock.Now()

	endpointSlices, err := clusterContext.GetEndpointSlices(m.config.Namespace, m.config.EndpointSelectorLabelName, m.config.EndpointSelectorLabelValue)
	if err != nil {
		log.Warnf("could not get prometheus metrics endpoint slices, abandoning custom prometheus scrape: %v", err)
		return
	}

	urls := getUrlsToScrape(endpointSlices, util.ExtractNodeNames(nodes))

	samples := scrapeUrls(urls, extractPrometheusMetricNames(m.config.Metrics), m.httpClient)

	updateMetrics(samples, m.config.Metrics, podNameToUtilisationData)

	taken := m.clock.Now().Sub(start)
	log.Infof("Scraped %d urls for custom usage metrics %s %s=%s in %s, got back %d samples", len(urls), m.config.Namespace, m.config.EndpointSelectorLabelName, m.config.EndpointSelectorLabelValue, taken, len(samples))
}

func updateMetrics(samples model.Vector, metrics []configuration.CustomUsageMetric, podNameToUtilisationData map[string]*domain.UtilisationData) {
	samplesByMetricName := groupSamplesBy(samples, model.MetricNameLabel)
	for _, metric := range metrics {
		metricSamples, exists := samplesByMetricName[model.LabelValue(metric.PrometheusMetricName)]
		if !exists {
			continue
		}
		updateMetric(metricSamples, metric, podNameToUtilisationData)
	}
}

func updateMetric(metricSamples model.Vector, metric configuration.CustomUsageMetric, podNameToUtilisationData map[string]*domain.UtilisationData) {
	metricSamplesByPod := groupSamplesBy(metricSamples, model.LabelName(metric.PrometheusPodNameLabel))
	for podName, podData := range podNameToUtilisationData {
		if metricPodSamples, exists := metricSamplesByPod[model.LabelValue(podName)]; exists {
			val := aggregateSamples(metricPodSamples, metric.AggregateType)
			if metric.Multiplier > 0 {
				val *= metric.Multiplier
			}
			podData.CurrentUsage[metric.Name] = toQuantity(val)
		}
	}
}

func extractPrometheusMetricNames(specs []configuration.CustomUsageMetric) []string {
	var result []string
	for _, spec := range specs {
		result = append(result, spec.PrometheusMetricName)
	}
	return result
}

func toQuantity(val float64) resource.Quantity {
	if isInteger(val) {
		return *resource.NewQuantity(int64(val), resource.DecimalSI)
	}
	return *resource.NewMilliQuantity(int64(val*1000.0), resource.DecimalSI)
}

func isInteger(val float64) bool {
	return math.Mod(val, 1.0) == 0
}
