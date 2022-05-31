package etcdhealthmonitor

import (
	"bufio"
	"context"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/G-Research/armada/internal/common/armadaerrors"
	"github.com/G-Research/armada/internal/common/logging"
	"github.com/G-Research/armada/internal/executor/configuration"
)

const etcdInUseSizeBytesMetricName string = "etcd_mvcc_db_total_size_in_use_in_bytes"
const etcdTotalSizeBytesMetricName string = "etcd_server_quota_backend_bytes"

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

// Store monitor information for a particular etcd instance.
type etcdInstanceMonitor struct {
	// When metrics was last collected for this instance.
	lastCheck time.Time
	// Error returned by the most recent attempt to scrape metrics for this instance.
	err error
	// Metrics collected from etcd (a map from metric name to value); see
	// https://etcd.io/docs/v3.5/op-guide/monitoring/
	metrics map[string]float64
}

func (e *etcdInstanceMonitor) deepCopy() *etcdInstanceMonitor {
	metricsCopy := make(map[string]float64, len(e.metrics))
	for key, value := range e.metrics {
		metricsCopy[key] = value
	}
	return &etcdInstanceMonitor{
		lastCheck: time.Unix(e.lastCheck.Unix(), e.lastCheck.UnixNano()),
		err:       e.err,
		metrics:   metricsCopy,
	}
}

// Return a new EtcdHealthMonitor that monitors the etcd instances at the given urls.
// Provide a http client, e.g., to use auth, or set client to nil to use the default client.
func New(etcConfiguration configuration.EtcdConfiguration, client *http.Client) (*EtcdHealthMonitor, error) {
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
	return rv, nil
}

// Run the service until ctx is cancelled.
func (srv *EtcdHealthMonitor) Run(ctx context.Context) {
	log.WithField("service", "EtcdHealthMonitor").Info("started ETCD health monitor")
	defer log.WithField("service", "EtcdHealthMonitor").Info("exited ETCD health monitor")

	ticker := time.NewTicker(srv.ScrapeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := srv.scrapeMetrics(ctx); err != nil {
				logging.WithStacktrace(log.WithField("service", "EtcdHealthMonitor"), err).Error("failed to scrape metrics from etcd")
			}
			// TODO Add prometheus metrics for success/fail + current values
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
	g, ctx := errgroup.WithContext(ctx)
	instances := srv.getInstances()
	for url, instance := range instances {
		url := url
		instance := instance
		g.Go(func() error {
			req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
			if err != nil {
				instance.err = err
				return errors.WithStack(err)
			}

			resp, err := srv.client.Do(req)
			if err != nil {
				instance.err = err
				return errors.WithStack(err)
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
			instance.metrics = metrics
			instance.lastCheck = time.Now()
			instance.err = nil
			return nil
		})
	}
	err := g.Wait()
	srv.updateInstances(instances)
	return err
}

func (srv *EtcdHealthMonitor) IsAtSoftHealthLimit() bool {
	currentFractionOfResourceInUse, err := srv.currentFractionOfResourceInUse()
	if err != nil {
		return true
	}
	return currentFractionOfResourceInUse >= srv.etcdConfiguration.FractionOfStorageInUseSoftLimit
}

func (srv *EtcdHealthMonitor) IsAtHardHealthLimit() bool {
	currentFractionOfResourceInUse, err := srv.currentFractionOfResourceInUse()
	if err != nil {
		return true
	}
	return currentFractionOfResourceInUse >= srv.etcdConfiguration.FractionOfStorageInUseHardLimit
}

// MaxFractionOfStorageInUse returns the maximum fraction of storage in use over all etcd instances.
func (srv *EtcdHealthMonitor) currentFractionOfResourceInUse() (float64, error) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	rv := 0.0
	available := 0
	for url, instance := range srv.instances {
		if instance == nil {
			return 0, errors.WithStack(&armadaerrors.ErrInvalidArgument{
				Name:    "instances",
				Value:   srv.instances,
				Message: "instance is nil",
			})
		}

		metricsAge := time.Since(instance.lastCheck)
		if metricsAge > srv.MetricsMaxAge {
			log.Warnf("skipping instance as max age is %s, but metrics for etcd instance at %s are of age %s; instance may be down",
				srv.MetricsMaxAge, url, metricsAge)
			continue
		}

		totalSize, ok := instance.metrics[etcdTotalSizeBytesMetricName]
		if !ok {
			log.Warnf("skipping instance %s as metric etcd_server_quota_backend_bytes not available", url)
			continue
		}

		inUse, ok := instance.metrics[etcdInUseSizeBytesMetricName]
		if !ok {
			log.Warnf("skipping instance %s as metric etcd_mvcc_db_total_size_in_use_in_bytes not available", url)
			continue
		}

		fractionInUse := inUse / totalSize
		if fractionInUse > rv {
			rv = fractionInUse
		}
		available++
	}

	if len(srv.instances) < srv.etcdConfiguration.MinimumAvailable {
		return 0, errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "instances",
			Value:   srv.instances,
			Message: "insufficient etcd metrics available",
		})
	}

	return rv, nil
}
