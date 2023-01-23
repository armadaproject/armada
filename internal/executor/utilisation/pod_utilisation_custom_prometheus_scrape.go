package utilisation

import (
	"net/http"
	"time"

	clusterContext "github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/util"
	"github.com/prometheus/common/model"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

type CustomPrometheusScrapeConfig struct {
	Namespace                  string
	EndpointSelectorLabelName  string
	EndpointSelectorLabelValue string
	Metrics                    []MetricSpec
}

type MetricSpec struct {
	Name                   string
	PrometheusMetricName   string
	PrometheusPodNameLabel string
	Type                   AggregateType
}

func fetchCustomStats(nodes []*v1.Node, podNameToUtilisationData map[string]*domain.UtilisationData, clusterContext clusterContext.ClusterContext) {
	config := CustomPrometheusScrapeConfig{
		Namespace:                  "gpu-operator",
		EndpointSelectorLabelName:  "app",
		EndpointSelectorLabelValue: "nvidia-dcgm-exporter",
		Metrics: []MetricSpec{
			{
				Name:                   domain.AcceleratorDutyCycle,
				PrometheusMetricName:   "DCGM_FI_DEV_GPU_UTIL",
				PrometheusPodNameLabel: "pod",
				Type:                   Mean,
			},
			{
				Name:                   "armadaproject.io/accelerator-memory-pct-util",
				PrometheusMetricName:   "DCGM_FI_DEV_MEM_COPY_UTIL",
				PrometheusPodNameLabel: "pod",
				Type:                   Mean,
			},
		},
	}

	endpointSlices, err := clusterContext.GetEndpointSlices(config.Namespace, config.EndpointSelectorLabelName, config.EndpointSelectorLabelValue)
	if err != nil {
		log.Warnf("could not get prometheus metrics endpoint slices, abandoning custom prometheus scrape: %v", err)
		return
	}

	log.Info("Found %d endpoint slices", len(endpointSlices))

	urls := getUrlsToScrape(endpointSlices, util.ExtractNodeNames(nodes))
	log.Info("Got these urls to scrape: %v", urls)

	client := http.Client{
		Timeout: 15 * time.Second,
	}

	samples := scrapeUrls(urls, extractMetricNames(config.Metrics), client)

	log.Info("Got %d samples in total", len(samples))
	samplesByMetricName := groupSamplesBy(samples, model.MetricNameLabel)
	for _, metric := range config.Metrics {
		metricSamples, exists := samplesByMetricName[model.LabelValue(metric.Name)]
		log.Info("Got %d samples for metric %s", len(metricSamples), metric.PrometheusMetricName)
		if !exists {
			continue
		}
		metricSamplesByPod := groupSamplesBy(metricSamples, model.LabelName(metric.PrometheusPodNameLabel))
		for podName, podData := range podNameToUtilisationData {
			if metricPodSamples, exists := metricSamplesByPod[model.LabelValue(podName)]; exists {
				podData.CurrentUsage[metric.Name] = *resource.NewQuantity(int64(aggregateSamples(metricPodSamples, metric.Type)), resource.DecimalExponent)
			}
		}
	}
}

func extractMetricNames(specs []MetricSpec) []string {
	var result []string
	for _, spec := range specs {
		result = append(result, spec.PrometheusMetricName)
	}
	return result
}
