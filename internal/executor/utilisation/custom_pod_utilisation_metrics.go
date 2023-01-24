package utilisation

import (
	"math"
	"net/http"
	"time"

	"github.com/prometheus/common/model"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	clusterContext "github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/util"
)

type CustomPodUtilisationMetrics struct {
	Namespace                  string
	EndpointSelectorLabelName  string
	EndpointSelectorLabelValue string
	Metrics                    []CustomPodUtilisationMetric
}

type CustomPodUtilisationMetric struct {
	Name                   string
	PrometheusMetricName   string
	PrometheusPodNameLabel string
	AggregateType          AggregateType
}

func fetchCustomStats(nodes []*v1.Node, podNameToUtilisationData map[string]*domain.UtilisationData, clusterContext clusterContext.ClusterContext) {
	config := CustomPodUtilisationMetrics{
		Namespace:                  "gpu-operator",
		EndpointSelectorLabelName:  "app",
		EndpointSelectorLabelValue: "nvidia-dcgm-exporter",
		Metrics: []CustomPodUtilisationMetric{
			{
				Name:                   domain.AcceleratorDutyCycle,
				PrometheusMetricName:   "DCGM_FI_DEV_GPU_UTIL",
				PrometheusPodNameLabel: "pod",
				AggregateType:          Mean,
			},
			{
				Name:                   "armadaproject.io/accelerator-memory-pct-util",
				PrometheusMetricName:   "DCGM_FI_DEV_MEM_COPY_UTIL",
				PrometheusPodNameLabel: "pod",
				AggregateType:          Mean,
			},
		},
	}

	endpointSlices, err := clusterContext.GetEndpointSlices(config.Namespace, config.EndpointSelectorLabelName, config.EndpointSelectorLabelValue)
	if err != nil {
		log.Warnf("could not get prometheus metrics endpoint slices, abandoning custom prometheus scrape: %v", err)
		return
	}
	log.Infof("Found %d endpoint slices", len(endpointSlices))

	urls := getUrlsToScrape(endpointSlices, util.ExtractNodeNames(nodes))
	log.Infof("Got these urls to scrape: %v", urls)

	client := http.Client{
		Timeout: 15 * time.Second,
	}

	samples := scrapeUrls(urls, extractPrometheusMetricNames(config.Metrics), &client)

	log.Infof("Got %d samples in total", len(samples))
	updateMetrics(samples, config.Metrics, podNameToUtilisationData)
}

func updateMetrics(samples model.Vector, metrics []CustomPodUtilisationMetric, podNameToUtilisationData map[string]*domain.UtilisationData) {
	samplesByMetricName := groupSamplesBy(samples, model.MetricNameLabel)
	for _, metric := range metrics {
		metricSamples, exists := samplesByMetricName[model.LabelValue(metric.PrometheusMetricName)]
		log.Infof("Got %d samples for metric %s", len(metricSamples), metric.PrometheusMetricName)
		if !exists {
			continue
		}
		updateMetric(metricSamples, metric, podNameToUtilisationData)
	}
}

func updateMetric(metricSamples model.Vector, metric CustomPodUtilisationMetric, podNameToUtilisationData map[string]*domain.UtilisationData) {
	metricSamplesByPod := groupSamplesBy(metricSamples, model.LabelName(metric.PrometheusPodNameLabel))
	for podName, podData := range podNameToUtilisationData {
		if metricPodSamples, exists := metricSamplesByPod[model.LabelValue(podName)]; exists {
			podData.CurrentUsage[metric.Name] = toQuantity(aggregateSamples(metricPodSamples, metric.AggregateType))
		}
	}
}

func extractPrometheusMetricNames(specs []CustomPodUtilisationMetric) []string {
	var result []string
	for _, spec := range specs {
		result = append(result, spec.PrometheusMetricName)
	}
	return result
}

func toQuantity(val float64) resource.Quantity {
	if isInteger(val) {
		return *resource.NewQuantity(int64(val), resource.DecimalExponent)
	}
	return *resource.NewMilliQuantity(int64(val*1000.0), resource.DecimalExponent)
}

func isInteger(val float64) bool {
	return math.Mod(val, 1.0) == 0
}
