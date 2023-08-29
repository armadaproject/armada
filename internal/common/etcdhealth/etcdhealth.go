package etcdhealth

import (
	"bufio"
	"context"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/common/healthmonitor"
	"github.com/armadaproject/armada/internal/common/logging"
)

const (
	etcdSizeInUseBytesMetricName string = "etcd_mvcc_db_total_size_in_use_in_bytes"
	etcdSizeBytesMetricName      string = "etcd_mvcc_db_total_size_in_bytes"
	etcdCapacityBytesMetricName  string = "etcd_server_quota_backend_bytes"
	etcdMemberUrl                string = "url"

	EtcdReplicaSizeInUseExceededReason string = "etcdReplicaSizeInUseExceeded"
	EtcdReplicaSizeExceededReason      string = "etcdReplicaSizeExceeded"
)

// EtcdReplicaHealthMonitor is a health monitor for monitoring the health of an individual etcd replica.
type EtcdReplicaHealthMonitor struct {
	// Url on which to scrape metrics.
	metricsUrl string
	// The cluster is considered unhealthy when for any replica in the cluster:
	// etcd_mvcc_db_total_size_in_use_in_bytes / etcd_server_quota_backend_bytes
	// > FractionOfStorageInUseLimit.
	fractionOfStorageInUseLimit float64
	// The cluster is considered unhealthy when for any replica in the cluster:
	// etcd_mvcc_db_total_size_in_bytes / etcd_server_quota_backend_bytes
	// > FractionOfStorageLimit.
	fractionOfStorageLimit float64
	// A replica is considered unavailable if the executor has failed to collect metrics from it for this amount of time.
	// The cluster is considered unhealthy if there are less than MinimumReplicasAvailable replicas available.
	replicaTimeout time.Duration
	// Interval with which to scrape metrics from the replica.
	scrapeInterval time.Duration

	// Prometheus metrics are prefixed with this.
	metricsPrefix string

	// HTTP client with which to scrape.
	client *http.Client

	// Time at which metrics collection was most recently attempted.
	timeOfMostRecentCollectionAttempt time.Time
	// Time at which metrics were most recently collected successfully.
	timeOfMostRecentSuccessfulCollectionAttempt time.Time
	// Relevant metrics scraped from etcd.
	etcdSizeInUseBytes float64
	etcdSizeBytes      float64
	etcdCapacityBytes  float64
	// Mutex protecting the above fields.
	mu sync.Mutex

	healthPrometheusDesc                                      *prometheus.Desc
	timeOfMostRecentCollectionAttemptPrometheusDesc           *prometheus.Desc
	timeOfMostRecentSuccessfulCollectionAttemptPrometheusDesc *prometheus.Desc
	sizeInUseFractionPrometheusDesc                           *prometheus.Desc
	sizeFractionPrometheusDesc                                *prometheus.Desc
}

func NewEtcdReplicaHealthMonitor(
	metricsUrl string,
	fractionOfStorageInUseLimit float64,
	fractionOfStorageLimit float64,
	replicaTimeout time.Duration,
	scrapeInterval time.Duration,
	client *http.Client,
) *EtcdReplicaHealthMonitor {
	return &EtcdReplicaHealthMonitor{
		metricsUrl:                  metricsUrl,
		fractionOfStorageInUseLimit: fractionOfStorageInUseLimit,
		fractionOfStorageLimit:      fractionOfStorageLimit,
		replicaTimeout:              replicaTimeout,
		scrapeInterval:              scrapeInterval,
		client:                      client,
	}
}

func (srv *EtcdReplicaHealthMonitor) WithMetricsPrefix(v string) *EtcdReplicaHealthMonitor {
	srv.metricsPrefix = v
	return srv
}

func (srv *EtcdReplicaHealthMonitor) IsHealthy() (bool, string, error) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.hasTimedOut() {
		return false, healthmonitor.TimedOutReason, nil
	}
	ok, reason := srv.isHealthy()
	return ok, reason, nil
}

func (srv *EtcdReplicaHealthMonitor) hasTimedOut() bool {
	return time.Since(srv.timeOfMostRecentSuccessfulCollectionAttempt) > srv.replicaTimeout
}

func (srv *EtcdReplicaHealthMonitor) isHealthy() (bool, string) {
	if srv.sizeInUseFraction() > srv.fractionOfStorageInUseLimit {
		return false, EtcdReplicaSizeInUseExceededReason
	}
	if srv.sizeFraction() > srv.fractionOfStorageLimit {
		return false, EtcdReplicaSizeExceededReason
	}
	return true, ""
}

func (srv *EtcdReplicaHealthMonitor) sizeInUseFraction() float64 {
	return srv.etcdSizeInUseBytes / srv.etcdCapacityBytes
}

func (srv *EtcdReplicaHealthMonitor) sizeFraction() float64 {
	return srv.etcdSizeBytes / srv.etcdCapacityBytes
}

func (srv *EtcdReplicaHealthMonitor) Run(ctx context.Context, log *logrus.Entry) error {
	srv.healthPrometheusDesc = prometheus.NewDesc(
		srv.metricsPrefix+"etcd_replica_health",
		"Shows the health of an etcd replica",
		[]string{etcdMemberUrl},
		nil,
	)
	srv.timeOfMostRecentCollectionAttemptPrometheusDesc = prometheus.NewDesc(
		srv.metricsPrefix+"etcd_replica_time_of_most_recent_metrics_collection_attempt",
		"Time of most recent health check.",
		[]string{etcdMemberUrl},
		nil,
	)
	srv.timeOfMostRecentSuccessfulCollectionAttemptPrometheusDesc = prometheus.NewDesc(
		srv.metricsPrefix+"etcd_replica_time_of_most_recent_successful_metrics_collection",
		"Time of most recent successful health check.",
		[]string{etcdMemberUrl},
		nil,
	)
	srv.sizeInUseFractionPrometheusDesc = prometheus.NewDesc(
		srv.metricsPrefix+"etcd_replica_size_in_use_fraction",
		"etcd_mvcc_db_total_size_in_use_in_bytes / etcd_server_quota_backend_bytes.",
		[]string{etcdMemberUrl},
		nil,
	)
	srv.sizeFractionPrometheusDesc = prometheus.NewDesc(
		srv.metricsPrefix+"etcd_replica_size_fraction",
		"etcd_mvcc_db_total_size_in_bytes / etcd_server_quota_backend_bytes.",
		[]string{etcdMemberUrl},
		nil,
	)

	log = log.WithField("service", "EtcdHealthMonitor")
	log.Info("starting etcd health monitor")
	defer log.Info("stopping etcd health monitor")
	ticker := time.NewTicker(srv.scrapeInterval)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			metrics, err := srv.scrape(ctx)
			srv.mu.Lock()
			srv.timeOfMostRecentCollectionAttempt = time.Now()
			if err != nil {
				logging.WithStacktrace(log, err).Errorf("failed to scrape etcd metrics from %s", srv.metricsUrl)
			} else {
				if srv.setSizeInUseBytesFromMetrics(metrics); err != nil {
					logging.WithStacktrace(log, err).Errorf("failed to scrape etcd metrics from %s", srv.metricsUrl)
				}
				if srv.setSizeBytesFromMetrics(metrics); err != nil {
					logging.WithStacktrace(log, err).Errorf("failed to scrape etcd metrics from %s", srv.metricsUrl)
				}
				if srv.setCapacityBytesFromMetrics(metrics); err != nil {
					logging.WithStacktrace(log, err).Errorf("failed to scrape etcd metrics from %s", srv.metricsUrl)
				}
				srv.timeOfMostRecentSuccessfulCollectionAttempt = srv.timeOfMostRecentCollectionAttempt
			}
			srv.mu.Unlock()
		}
	}
}

func (srv *EtcdReplicaHealthMonitor) scrape(ctx context.Context) (map[string]float64, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", srv.metricsUrl, nil)
	if err != nil {
		return nil, err
	}
	resp, err := srv.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	metrics := make(map[string]float64)
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if i := strings.Index(line, "#"); i >= 0 && i < len(line) {
			line = line[:i]
		}
		if len(line) == 0 {
			continue
		}
		keyVal := strings.Split(line, " ")
		if len(keyVal) != 2 {
			continue
		}
		key := keyVal[0]
		val, err := strconv.ParseFloat(keyVal[1], 64)
		if err != nil {
			continue
		}
		metrics[key] = val
	}
	return metrics, nil
}

func (srv *EtcdReplicaHealthMonitor) setSizeInUseBytesFromMetrics(metrics map[string]float64) error {
	f, ok := metrics[etcdSizeInUseBytesMetricName]
	if !ok {
		return errors.Errorf("metric unavailable: %s", etcdSizeInUseBytesMetricName)
	}
	srv.etcdSizeInUseBytes = f
	return nil
}

func (srv *EtcdReplicaHealthMonitor) setSizeBytesFromMetrics(metrics map[string]float64) error {
	f, ok := metrics[etcdSizeBytesMetricName]
	if !ok {
		return errors.Errorf("metric unavailable: %s", etcdSizeBytesMetricName)
	}
	srv.etcdSizeBytes = f
	return nil
}

func (srv *EtcdReplicaHealthMonitor) setCapacityBytesFromMetrics(metrics map[string]float64) error {
	f, ok := metrics[etcdCapacityBytesMetricName]
	if !ok {
		return errors.Errorf("metric unavailable: %s", etcdCapacityBytesMetricName)
	}
	srv.etcdCapacityBytes = f
	return nil
}

func (srv *EtcdReplicaHealthMonitor) Describe(c chan<- *prometheus.Desc) {
	c <- srv.healthPrometheusDesc
	c <- srv.timeOfMostRecentCollectionAttemptPrometheusDesc
	c <- srv.timeOfMostRecentSuccessfulCollectionAttemptPrometheusDesc
	c <- srv.sizeInUseFractionPrometheusDesc
	c <- srv.sizeFractionPrometheusDesc
}

func (srv *EtcdReplicaHealthMonitor) Collect(c chan<- prometheus.Metric) {
	srv.mu.Lock()
	resultOfMostRecentHealthCheck := 0.0
	if ok, _ := srv.isHealthy(); ok {
		resultOfMostRecentHealthCheck = 1.0
	}
	timeOfMostRecentCollectionAttempt := srv.timeOfMostRecentCollectionAttempt
	timeOfMostRecentSuccessfulCollectionAttempt := srv.timeOfMostRecentSuccessfulCollectionAttempt
	sizeInUseFraction := srv.sizeInUseFraction()
	sizeFraction := srv.sizeFraction()
	srv.mu.Unlock()

	c <- prometheus.MustNewConstMetric(
		srv.healthPrometheusDesc,
		prometheus.GaugeValue,
		resultOfMostRecentHealthCheck,
		srv.metricsUrl,
	)
	c <- prometheus.MustNewConstMetric(
		srv.timeOfMostRecentCollectionAttemptPrometheusDesc,
		prometheus.CounterValue,
		float64(timeOfMostRecentCollectionAttempt.Unix()),
		srv.metricsUrl,
	)
	c <- prometheus.MustNewConstMetric(
		srv.timeOfMostRecentSuccessfulCollectionAttemptPrometheusDesc,
		prometheus.CounterValue,
		float64(timeOfMostRecentSuccessfulCollectionAttempt.Unix()),
		srv.metricsUrl,
	)
	c <- prometheus.MustNewConstMetric(
		srv.sizeInUseFractionPrometheusDesc,
		prometheus.GaugeValue,
		sizeInUseFraction,
		srv.metricsUrl,
	)
	c <- prometheus.MustNewConstMetric(
		srv.sizeFractionPrometheusDesc,
		prometheus.GaugeValue,
		sizeFraction,
		srv.metricsUrl,
	)
}
