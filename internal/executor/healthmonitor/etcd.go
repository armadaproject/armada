package healthmonitor

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/executor/configuration"
	"github.com/armadaproject/armada/internal/executor/metrics"
)

const (
	etcdInUseSizeBytesMetricName string = "etcd_mvcc_db_total_size_in_use_in_bytes"
	etcdTotalSizeBytesMetricName string = "etcd_server_quota_backend_bytes"
	etcdMemberUrl                string = "url"
)

type EtcdLimitHealthMonitor interface {
	IsWithinSoftHealthLimit() bool
	IsWithinHardHealthLimit() bool
}

// EtcdHealthMonitor is a service for monitoring the health of etcd.
// It continually scrapes metrics from one of more etcd instances
// and provides a method for checking the fraction of storage in use.
type EtcdHealthMonitor struct {
	// Maps instance URL to information about that particular instance.
	instances map[string]*etcdInstanceMonitor
	// HTTP client used to make requests.
	client *http.Client
	// Time after which we consider instances to be down if no metrics have been collected successfully.
	// Defaults to 2 minutes.
	MetricsMaxAge time.Duration
	// Interval at which to scrape metrics from etcd.
	// Defaults to 5 second.
	ScrapeInterval time.Duration
	// Configuration for etcd
	etcdConfiguration configuration.EtcdConfiguration
	mu                sync.Mutex
}

var etcdInstanceUpDesc = prometheus.NewDesc(
	metrics.ArmadaExecutorMetricsPrefix+"_etcd_instance_up",
	"Shows if an etcd instance is sufficiently live to get metrics from",
	[]string{etcdMemberUrl}, nil,
)

var etcdInstanceHealthCheckTimeDesc = prometheus.NewDesc(
	metrics.ArmadaExecutorMetricsPrefix+"etcd_instance_health_check_time",
	"Time of the last successful health check scrape",
	[]string{etcdMemberUrl}, nil,
)

var etcdInstanceInUseFractionDesc = prometheus.NewDesc(
	metrics.ArmadaExecutorMetricsPrefix+"etcd_instance_current_in_use_fraction",
	"Current fraction of the etcd instance that is in use",
	[]string{etcdMemberUrl}, nil,
)

// Return a new EtcdHealthMonitor that monitors the etcd instances at the given urls.
// Provide a http client, e.g., to use auth, or set client to nil to use the default client.
func NewEtcdHealthMonitor(etcConfiguration configuration.EtcdConfiguration, client *http.Client) (*EtcdHealthMonitor, error) {
	if len(etcConfiguration.MetricUrls) == 0 {
		return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "urls",
			Value:   etcConfiguration.MetricUrls,
			Message: "no URLs provided",
		})
	}
	if client == nil {
		client = http.DefaultClient
	}
	rv := &EtcdHealthMonitor{
		MetricsMaxAge:     2 * time.Minute,
		ScrapeInterval:    time.Second * 5,
		instances:         make(map[string]*etcdInstanceMonitor),
		client:            client,
		etcdConfiguration: etcConfiguration,
	}
	for _, url := range etcConfiguration.MetricUrls {
		rv.instances[url] = &etcdInstanceMonitor{
			metrics: make(map[string]float64),
		}
	}
	go rv.Run(context.Background())
	prometheus.MustRegister(rv)
	return rv, nil
}

func (srv *EtcdHealthMonitor) Describe(desc chan<- *prometheus.Desc) {
	desc <- etcdInstanceUpDesc
	desc <- etcdInstanceHealthCheckTimeDesc
	desc <- etcdInstanceInUseFractionDesc
}

func (srv *EtcdHealthMonitor) Collect(metrics chan<- prometheus.Metric) {
	for url, instance := range srv.getInstances() {
		currentFraction, err := srv.getInstanceCurrentFractionOfResourceInUse(instance)
		metrics <- prometheus.MustNewConstMetric(etcdInstanceHealthCheckTimeDesc, prometheus.CounterValue, float64(instance.lastCheck.Unix()), url)
		if err != nil {
			metrics <- prometheus.MustNewConstMetric(etcdInstanceUpDesc, prometheus.GaugeValue, 0, url)
			prometheus.NewInvalidMetric(etcdInstanceInUseFractionDesc, err)
		} else {
			metrics <- prometheus.MustNewConstMetric(etcdInstanceUpDesc, prometheus.GaugeValue, 1, url)
			metrics <- prometheus.MustNewConstMetric(etcdInstanceInUseFractionDesc, prometheus.GaugeValue, currentFraction, url)
		}
	}
}

// Run the service until ctx is cancelled.
func (srv *EtcdHealthMonitor) Run(ctx context.Context) {
	log.WithField("service", "EtcdHealthMonitor").Info("started ETCD health monitor")
	defer log.WithField("service", "EtcdHealthMonitor").Info("exited ETCD health monitor")

	taskDurationHistogram := promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    metrics.ArmadaExecutorMetricsPrefix + "etcd_health_check_latency_seconds",
			Help:    "Background loop etcd health check latency in seconds",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 15),
		})

	ticker := time.NewTicker(srv.ScrapeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			start := time.Now()
			if err := srv.scrapeMetrics(ctx); err != nil {
				logging.WithStacktrace(log.WithField("service", "EtcdHealthMonitor"), err).Error("failed to scrape metrics from etcd")
			}
			duration := time.Since(start)
			taskDurationHistogram.Observe(duration.Seconds())
		}
	}
}

func (srv *EtcdHealthMonitor) getInstances() map[string]*etcdInstanceMonitor {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	current := srv.instances
	instancesCopy := make(map[string]*etcdInstanceMonitor, len(current))

	for url, instance := range current {
		instancesCopy[url] = instance.deepCopy()
	}

	return instancesCopy
}

func (srv *EtcdHealthMonitor) updateInstances(updatedInstances map[string]*etcdInstanceMonitor) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	srv.instances = updatedInstances
}

// ScrapeMetrics collects metrics for all etcd instances.
func (srv *EtcdHealthMonitor) scrapeMetrics(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	wg := sync.WaitGroup{}
	instances := srv.getInstances()
	for url, instance := range instances {
		wg.Add(1)
		url := url
		instance := instance
		go func() {
			defer wg.Done()
			req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
			if err != nil {
				instance.err = err
				return
			}

			resp, err := srv.client.Do(req)
			if err != nil {
				instance.err = err
				return
			}
			defer resp.Body.Close()

			receivedMetrics := make(map[string]float64)
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
				receivedMetrics[key] = val
			}
			instance.metrics = receivedMetrics
			instance.lastCheck = time.Now()
			instance.err = nil
		}()
	}

	wg.Wait()
	srv.updateInstances(instances)

	for _, instance := range instances {
		if instance.err != nil {
			return instance.err
		}
	}
	return nil
}

func (srv *EtcdHealthMonitor) IsWithinSoftHealthLimit() bool {
	currentFractionOfResourceInUse, err := srv.currentFractionOfResourceInUse()
	if err != nil {
		return false
	}
	return currentFractionOfResourceInUse < srv.etcdConfiguration.FractionOfStorageInUseSoftLimit
}

func (srv *EtcdHealthMonitor) IsWithinHardHealthLimit() bool {
	currentFractionOfResourceInUse, err := srv.currentFractionOfResourceInUse()
	if err != nil {
		return false
	}
	return currentFractionOfResourceInUse < srv.etcdConfiguration.FractionOfStorageInUseHardLimit
}

// MaxFractionOfStorageInUse returns the maximum fraction of storage in use over all etcd instances.
func (srv *EtcdHealthMonitor) currentFractionOfResourceInUse() (float64, error) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	maxCurrentFractionInUse := 0.0
	available := 0
	for url, instance := range srv.instances {
		fractionInUse, err := srv.getInstanceCurrentFractionOfResourceInUse(instance)
		if err != nil {
			log.Warnf("skipping etcd instance %s as %s", url, err)
			continue
		}
		if fractionInUse > maxCurrentFractionInUse {
			maxCurrentFractionInUse = fractionInUse
		}
		available++
	}

	if available < srv.etcdConfiguration.MinimumAvailable {
		return 0, errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "instances",
			Value:   srv.instances,
			Message: "insufficient etcd metrics available",
		})
	}

	return maxCurrentFractionInUse, nil
}

func (srv *EtcdHealthMonitor) getInstanceCurrentFractionOfResourceInUse(instance *etcdInstanceMonitor) (float64, error) {
	if instance == nil {
		return 0, fmt.Errorf("instance is nil")
	}

	if instance.lastCheck.IsZero() {
		return 0, fmt.Errorf("no scrape has ever occurred for this instance, possibly etcd health check is still initialising")
	}

	metricsAge := time.Since(instance.lastCheck)
	if metricsAge > srv.MetricsMaxAge {
		return 0, fmt.Errorf("metrics for etcd instance are too old current age %s max age %s; instance may be down", metricsAge, srv.MetricsMaxAge)
	}

	totalSize, ok := instance.metrics[etcdTotalSizeBytesMetricName]
	if !ok {
		return 0, fmt.Errorf("metric etcd_server_quota_backend_bytes not available")
	}

	inUse, ok := instance.metrics[etcdInUseSizeBytesMetricName]
	if !ok {
		return 0, fmt.Errorf("metric etcd_mvcc_db_total_size_in_use_in_bytes not available")
	}

	return inUse / totalSize, nil
}
