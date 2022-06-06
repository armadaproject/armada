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
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/G-Research/armada/internal/common/armadaerrors"
	"github.com/G-Research/armada/internal/common/logging"
)

// EtcdHealthMonitor is a service for monitoring the health of etcd.
// It continually scrapes metrics from one of more etcd instances
// and provides a method for checking the fraction of storage in use.
type EtcdHealthMonitor struct {
	// Maps instance URL to information about that particular instance.
	instances map[string]*etcdInstanceMonitor
	// HTTP client used to make requests.
	client *http.Client
	// Time after which we consider instances to be down if no metrics have been collected successfully.
	// Defaults to 5 minutes.
	MetricsMaxAge time.Duration
	// Interval at which to scrape metrics from etcd.
	// Defaults to 1 second.
	ScrapeInterval time.Duration
	// If provided, is used by run.
	Logger *logrus.Logger
	mu     sync.Mutex
}

// Store monitor information for a particular etcd instance.
type etcdInstanceMonitor struct {
	// When metrics was last collected for this instance.
	lastCheck time.Time
	// Error returned by the most recent attempt to scrape metrics for this instance.
	err error
	// Metrics collected from etcd (a map from metric name to value); see
	// https://etcd.io/docs/v3.1/op-guide/monitoring/
	metrics map[string]float64
}

// Return a new EtcdHealthMonitor that monitors the etcd instances at the given urls.
// Provide a http client, e.g., to use auth, or set client to nil to use the default client.
func New(urls []string, client *http.Client) (*EtcdHealthMonitor, error) {
	if len(urls) == 0 {
		return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "urls",
			Value:   urls,
			Message: "no URLs provided",
		})
	}
	if client == nil {
		client = http.DefaultClient
	}
	rv := &EtcdHealthMonitor{
		MetricsMaxAge:  5 * time.Minute,
		ScrapeInterval: time.Second,
		instances:      make(map[string]*etcdInstanceMonitor),
		client:         client,
	}
	for _, url := range urls {
		rv.instances[url] = &etcdInstanceMonitor{
			metrics: make(map[string]float64),
		}
	}
	return rv, nil
}

// Run the service until ctx is cancelled.
func (srv *EtcdHealthMonitor) Run(ctx context.Context) error {
	var log *logrus.Entry
	if srv.Logger == nil {
		log = logrus.StandardLogger().WithField("service", "EtcdHealthMonitor")
	} else {
		log = srv.Logger.WithField("service", "EtcdHealthMonitor")
	}
	log.Info("started")
	defer log.Info("exited")

	// log periodically
	lastLogged := time.Now()

	ticker := time.NewTicker(srv.ScrapeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := srv.ScrapeMetrics(ctx); err != nil {
				logging.WithStacktrace(log, err).Error("failed to scrape metrics from etcd")
			}
			if time.Since(lastLogged) > 10*time.Second {
				v, err := srv.MaxFractionOfStorageInUse()
				lastLogged = time.Now()
				if err != nil {
					logging.WithStacktrace(log, err).Error("failed to compute MaxFractionOfStorageInUse")
				} else {
					log.WithField("MaxFractionOfStorageInUse", v).Info("scraped metrics from etcd")
				}
			}
		}
	}
}

// ScrapeMetrics collects metrics for all etcd instances.
func (srv *EtcdHealthMonitor) ScrapeMetrics(ctx context.Context) error {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	g, ctx := errgroup.WithContext(ctx)
	for url, instance := range srv.instances {
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
	return g.Wait()
}

// MaxFractionOfStorageInUse returns the maximum fraction of storage in use over all etcd instances.
func (srv *EtcdHealthMonitor) MaxFractionOfStorageInUse() (float64, error) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	if len(srv.instances) == 0 {
		return 0, errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "instances",
			Value:   srv.instances,
			Message: "no metrics available",
		})
	}

	rv := 0.0
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
			err := instance.err
			if err == nil {
				err = errors.New("unknown error")
			}
			return 0, errors.WithStack(errors.WithMessagef(
				err,
				"max age is %s, but metrics for etcd instance at %s are of age %s; instance may be down",
				srv.MetricsMaxAge, url, metricsAge,
			))
		}

		totalSize, ok := instance.metrics["etcd_server_quota_backend_bytes"]
		if !ok {
			return 0, errors.WithStack(&armadaerrors.ErrInvalidArgument{
				Name:    "metrics",
				Value:   instance.metrics,
				Message: "metric etcd_server_quota_backend_bytes not available",
			})
		}

		inUse, ok := instance.metrics["etcd_mvcc_db_total_size_in_use_in_bytes"]
		if !ok {
			return 0, errors.WithStack(&armadaerrors.ErrInvalidArgument{
				Name:    "metrics",
				Value:   instance.metrics,
				Message: "metric etcd_mvcc_db_total_size_in_use_in_bytes not available",
			})
		}

		fractionInUse := inUse / totalSize
		if fractionInUse > rv {
			rv = fractionInUse
		}
	}
	return rv, nil
}
