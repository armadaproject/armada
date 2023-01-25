package utilisation

import (
	"math"
	"net/http"

	"github.com/prometheus/common/model"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/executor/configuration"
	clusterContext "github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/util"
)

type podUtilisationCustomMetrics struct {
	httpClient *http.Client
	config     *configuration.CustomPodUtilisationMetrics
}

func newPodUtilisationCustomMetrics(httpClient *http.Client, config *configuration.CustomPodUtilisationMetrics) *podUtilisationCustomMetrics {
	return &podUtilisationCustomMetrics{httpClient: httpClient, config: config}
}

func (m *podUtilisationCustomMetrics) fetch(nodes []*v1.Node, podNameToUtilisationData map[string]*domain.UtilisationData, clusterContext clusterContext.ClusterContext) {
	endpointSlices, err := clusterContext.GetEndpointSlices(m.config.Namespace, m.config.EndpointSelectorLabelName, m.config.EndpointSelectorLabelValue)
	if err != nil {
		log.Warnf("could not get prometheus metrics endpoint slices, abandoning custom prometheus scrape: %v", err)
		return
	}

	urls := getUrlsToScrape(endpointSlices, util.ExtractNodeNames(nodes))

	samples := scrapeUrls(urls, extractPrometheusMetricNames(m.config.Metrics), m.httpClient)

	updateMetrics(samples, m.config.Metrics, podNameToUtilisationData)
}

func updateMetrics(samples model.Vector, metrics []configuration.CustomPodUtilisationMetric, podNameToUtilisationData map[string]*domain.UtilisationData) {
	samplesByMetricName := groupSamplesBy(samples, model.MetricNameLabel)
	for _, metric := range metrics {
		metricSamples, exists := samplesByMetricName[model.LabelValue(metric.PrometheusMetricName)]
		if !exists {
			continue
		}
		updateMetric(metricSamples, metric, podNameToUtilisationData)
	}
}

func updateMetric(metricSamples model.Vector, metric configuration.CustomPodUtilisationMetric, podNameToUtilisationData map[string]*domain.UtilisationData) {
	metricSamplesByPod := groupSamplesBy(metricSamples, model.LabelName(metric.PrometheusPodNameLabel))
	for podName, podData := range podNameToUtilisationData {
		if metricPodSamples, exists := metricSamplesByPod[model.LabelValue(podName)]; exists {
			podData.CurrentUsage[metric.Name] = toQuantity(aggregateSamples(metricPodSamples, metric.AggregateType))
		}
	}
}

func extractPrometheusMetricNames(specs []configuration.CustomPodUtilisationMetric) []string {
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
