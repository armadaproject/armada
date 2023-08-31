package etcdhealth

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/common/healthmonitor"
	"github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/metrics"
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
	// Name of the replica being scraped, e.g., its url.
	// Included in exported Prometheus metrics.
	name string
	// Exported Prometheus metrics are prefixed with this.
	metricsPrefix string

	// The cluster is considered unhealthy when for any replica in the cluster:
	// etcd_mvcc_db_total_size_in_use_in_bytes / etcd_server_quota_backend_bytes > FractionOfStorageInUseLimit.
	fractionOfStorageInUseLimit float64
	// The cluster is considered unhealthy when for any replica in the cluster:
	// etcd_mvcc_db_total_size_in_bytes / etcd_server_quota_backend_bytes > FractionOfStorageLimit.
	fractionOfStorageLimit float64
	// A replica is considered unavailable if the executor has failed to collect metrics from it for this amount of time.
	replicaTimeout time.Duration
	// Interval with which to scrape metrics.
	scrapeInterval time.Duration

	// Time at which metrics collection was most recently attempted.
	timeOfMostRecentCollectionAttempt time.Time
	// Time at which metrics were most recently collected successfully.
	timeOfMostRecentSuccessfulCollectionAttempt time.Time

	// Relevant metrics scraped from etcd.
	etcdSizeInUseBytes float64
	etcdSizeBytes      float64
	etcdCapacityBytes  float64

	// Prometheus metrics.
	healthPrometheusDesc                                      *prometheus.Desc
	timeOfMostRecentCollectionAttemptPrometheusDesc           *prometheus.Desc
	timeOfMostRecentSuccessfulCollectionAttemptPrometheusDesc *prometheus.Desc
	sizeInUseFractionPrometheusDesc                           *prometheus.Desc
	sizeFractionPrometheusDesc                                *prometheus.Desc

	metricsCollectionDelayBucketsStart  float64
	metricsCollectionDelayBucketsFactor float64
	metricsCollectionDelayBucketsCount  int
	metricsCollectionDelayHistogram     prometheus.Histogram

	// Providing etcd metrics used for the health check.
	metricsProvider metrics.MetricsProvider

	// Used to block until the next metrics collection.
	watchers []chan struct{}

	// Mutex protecting the above fields.
	mu sync.Mutex
}

func NewEtcdReplicaHealthMonitor(
	name string,
	fractionOfStorageInUseLimit float64,
	fractionOfStorageLimit float64,
	replicaTimeout time.Duration,
	scrapeInterval time.Duration,
	metricsCollectionDelayBucketsStart float64,
	metricsCollectionDelayBucketsFactor float64,
	metricsCollectionDelayBucketsCount int,
	metricsProvider metrics.MetricsProvider,
) *EtcdReplicaHealthMonitor {
	return &EtcdReplicaHealthMonitor{
		name:                                name,
		fractionOfStorageInUseLimit:         fractionOfStorageInUseLimit,
		fractionOfStorageLimit:              fractionOfStorageLimit,
		replicaTimeout:                      replicaTimeout,
		scrapeInterval:                      scrapeInterval,
		metricsCollectionDelayBucketsStart:  metricsCollectionDelayBucketsStart,
		metricsCollectionDelayBucketsFactor: metricsCollectionDelayBucketsFactor,
		metricsCollectionDelayBucketsCount:  metricsCollectionDelayBucketsCount,
		metricsProvider:                     metricsProvider,
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
		return false, healthmonitor.UnavailableReason, nil
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
	srv.initialise()
	log = log.WithField("service", "EtcdHealthMonitor")
	log.Info("starting etcd health monitor")
	defer log.Info("stopping etcd health monitor")
	ticker := time.NewTicker(srv.scrapeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			t := time.Now()
			metrics, err := srv.metricsProvider.Collect(ctx, log)
			srv.mu.Lock()
			srv.timeOfMostRecentCollectionAttempt = time.Now()
			if err != nil {
				logging.WithStacktrace(log, err).Errorf("failed to scrape etcd metrics from %s", srv.name)
			} else {
				success := true
				if err := srv.setSizeInUseBytesFromMetrics(metrics); err != nil {
					success = false
					logging.WithStacktrace(log, err).Errorf("failed to scrape etcd metrics from %s", srv.name)
				}
				if err := srv.setSizeBytesFromMetrics(metrics); err != nil {
					success = false
					logging.WithStacktrace(log, err).Errorf("failed to scrape etcd metrics from %s", srv.name)
				}
				if err := srv.setCapacityBytesFromMetrics(metrics); err != nil {
					success = false
					logging.WithStacktrace(log, err).Errorf("failed to scrape etcd metrics from %s", srv.name)
				}
				if success {
					srv.timeOfMostRecentSuccessfulCollectionAttempt = srv.timeOfMostRecentCollectionAttempt
					srv.metricsCollectionDelayHistogram.Observe(floatingPointSecondsFromDuration(time.Since(t)))
				}
			}

			// Unblock any threads waiting for collection to finish.
			for _, c := range srv.watchers {
				close(c)
			}
			srv.watchers = nil
			srv.mu.Unlock()
		}
	}
}

func floatingPointSecondsFromDuration(d time.Duration) float64 {
	return float64(d) / 1e9
}

func (srv *EtcdReplicaHealthMonitor) initialise() {
	srv.healthPrometheusDesc = prometheus.NewDesc(
		srv.metricsPrefix+"etcd_replica_health",
		"Shows the health of an etcd replica",
		[]string{etcdMemberUrl},
		nil,
	)
	srv.timeOfMostRecentCollectionAttemptPrometheusDesc = prometheus.NewDesc(
		srv.metricsPrefix+"etcd_replica_time_of_most_recent_metrics_collection_attempt",
		"Time of most recent metrics collection attempt.",
		[]string{etcdMemberUrl},
		nil,
	)
	srv.timeOfMostRecentSuccessfulCollectionAttemptPrometheusDesc = prometheus.NewDesc(
		srv.metricsPrefix+"etcd_replica_time_of_most_recent_successful_metrics_collection",
		"Time of most recent successful metrics collection.",
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
	srv.metricsCollectionDelayHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: srv.metricsPrefix + "etcd_replica_metrics_collection_delay_seconds",
		Help: "Delay in seconds of collecting metrics from this etcd replica.",
		Buckets: prometheus.ExponentialBuckets(
			srv.metricsCollectionDelayBucketsStart,
			srv.metricsCollectionDelayBucketsFactor,
			srv.metricsCollectionDelayBucketsCount,
		),
	})
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

// BlockUntilNextMetricsCollection blocks until the next metrics collection has completed,
// or until ctx is cancelled, whichever occurs first.
func (srv *EtcdReplicaHealthMonitor) BlockUntilNextMetricsCollection(ctx context.Context) {
	c := make(chan struct{})
	srv.mu.Lock()
	srv.watchers = append(srv.watchers, c)
	srv.mu.Unlock()
	select {
	case <-ctx.Done():
		return
	case <-c:
		return
	}
}

func (srv *EtcdReplicaHealthMonitor) Describe(c chan<- *prometheus.Desc) {
	c <- srv.healthPrometheusDesc
	c <- srv.timeOfMostRecentCollectionAttemptPrometheusDesc
	c <- srv.timeOfMostRecentSuccessfulCollectionAttemptPrometheusDesc
	c <- srv.sizeInUseFractionPrometheusDesc
	c <- srv.sizeFractionPrometheusDesc
	srv.metricsCollectionDelayHistogram.Describe(c)
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
		srv.name,
	)
	c <- prometheus.MustNewConstMetric(
		srv.timeOfMostRecentCollectionAttemptPrometheusDesc,
		prometheus.CounterValue,
		float64(timeOfMostRecentCollectionAttempt.Unix()),
		srv.name,
	)
	c <- prometheus.MustNewConstMetric(
		srv.timeOfMostRecentSuccessfulCollectionAttemptPrometheusDesc,
		prometheus.CounterValue,
		float64(timeOfMostRecentSuccessfulCollectionAttempt.Unix()),
		srv.name,
	)
	c <- prometheus.MustNewConstMetric(
		srv.sizeInUseFractionPrometheusDesc,
		prometheus.GaugeValue,
		sizeInUseFraction,
		srv.name,
	)
	c <- prometheus.MustNewConstMetric(
		srv.sizeFractionPrometheusDesc,
		prometheus.GaugeValue,
		sizeFraction,
		srv.name,
	)
	srv.metricsCollectionDelayHistogram.Collect(c)
}
