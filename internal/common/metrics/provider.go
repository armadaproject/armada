package metrics

import (
	"bufio"
	"context"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type MetricsProvider interface {
	Collect(context.Context, *logrus.Entry) (map[string]float64, error)
}

type ManualMetricsProvider struct {
	metrics         map[string]float64
	collectionDelay time.Duration
	mu              sync.Mutex
}

func (srv *ManualMetricsProvider) WithMetrics(metrics map[string]float64) *ManualMetricsProvider {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	srv.metrics = metrics
	return srv
}

func (srv *ManualMetricsProvider) WithCollectionDelay(d time.Duration) *ManualMetricsProvider {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	srv.collectionDelay = d
	return srv
}

func (srv *ManualMetricsProvider) Collect(_ context.Context, _ *logrus.Entry) (map[string]float64, error) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.collectionDelay != 0 {
		time.Sleep(srv.collectionDelay)
	}
	return srv.metrics, nil
}

// HttpMetricsProvider is a metrics provider scraping metrics from a url.
type HttpMetricsProvider struct {
	url    string
	client *http.Client
}

func NewHttpMetricsProvider(url string, client *http.Client) *HttpMetricsProvider {
	if client == nil {
		client = http.DefaultClient
	}
	return &HttpMetricsProvider{
		url:    url,
		client: client,
	}
}

func (srv *HttpMetricsProvider) Collect(ctx context.Context, _ *logrus.Entry) (map[string]float64, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", srv.url, nil)
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
