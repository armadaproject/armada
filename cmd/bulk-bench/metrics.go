package main

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"k8s.io/client-go/kubernetes"
)

// apiserverMetrics scrapes the apiserver's own /metrics endpoint before, during
// and after a run, so the report says what the insert load did to the control
// plane rather than only how many creates per second the client managed.
//
// Only a handful of series are needed, so the text exposition format is parsed
// on demand instead of pulling in a metrics library.
type apiserverMetrics struct {
	client   kubernetes.Interface
	interval time.Duration
	enabled  bool
	reason   string

	before     string
	beforeTime time.Time
	after      string
	afterTime  time.Time

	mu           sync.Mutex
	inflightPeak float64

	stopCh chan struct{}
	doneCh chan struct{}
}

type apiserverReport struct {
	Enabled              bool    `json:"enabled"`
	Reason               string  `json:"reason,omitempty"`
	WindowSeconds        float64 `json:"windowSeconds"`
	PostRequests         float64 `json:"postRequests"`
	PostRatePerSecond    float64 `json:"postRatePerSecond"`
	PostMeanMs           float64 `json:"postMeanMs"`
	Code201              float64 `json:"code201"`
	Code429              float64 `json:"code429"`
	Code5xx              float64 `json:"code5xx"`
	EtcdCreateMeanMs     float64 `json:"etcdCreateMeanMs"`
	WebhookAdmitMeanMs   float64 `json:"webhookAdmitMeanMs"`
	APFRejected          float64 `json:"apfRejected"`
	AllRequestsPerSecond float64 `json:"allRequestsPerSecond"`
	InflightMutatingPeak float64 `json:"inflightMutatingPeak"`
	CPUCoresMean         float64 `json:"cpuCoresMean"`
	RSSAfterBytes        float64 `json:"rssAfterBytes"`
	PodsInStorageBefore  float64 `json:"podsInStorageBefore"`
	PodsInStorageAfter   float64 `json:"podsInStorageAfter"`
	DBSizeBytesBefore    float64 `json:"dbSizeBytesBefore"`
	DBSizeBytesAfter     float64 `json:"dbSizeBytesAfter"`
}

func newApiserverMetrics(client kubernetes.Interface, enabled bool, interval time.Duration) *apiserverMetrics {
	m := &apiserverMetrics{client: client, interval: interval, enabled: enabled}
	if !enabled {
		m.reason = "disabled by -apiserver-metrics=false"
		return m
	}
	if interval <= 0 {
		m.interval = time.Second
	}
	return m
}

func (m *apiserverMetrics) Start() {
	if !m.enabled {
		return
	}
	raw, err := m.scrape()
	if err != nil {
		m.enabled = false
		m.reason = fmt.Sprintf("scrape /metrics: %v", err)
		return
	}
	m.before, m.beforeTime = raw, time.Now()
	m.stopCh, m.doneCh = make(chan struct{}), make(chan struct{})
	go m.poll()
}

// poll samples the inflight gauge; counters only need a before and after
// snapshot, but saturation peaks are invisible in those.
func (m *apiserverMetrics) poll() {
	defer close(m.doneCh)
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			raw, err := m.scrape()
			if err != nil {
				continue
			}
			value := seriesSum(raw, "apiserver_current_inflight_requests", `request_kind="mutating"`)
			m.mu.Lock()
			if value > m.inflightPeak {
				m.inflightPeak = value
			}
			m.mu.Unlock()
		}
	}
}

func (m *apiserverMetrics) Stop() *apiserverReport {
	if !m.enabled {
		return &apiserverReport{Enabled: false, Reason: m.reason}
	}
	close(m.stopCh)
	<-m.doneCh
	raw, err := m.scrape()
	if err != nil {
		return &apiserverReport{Enabled: false, Reason: fmt.Sprintf("final scrape /metrics: %v", err)}
	}
	m.after, m.afterTime = raw, time.Now()

	window := m.afterTime.Sub(m.beforeTime)
	objectsMetric := firstMetric(m.after, "apiserver_resource_objects", "apiserver_storage_objects")
	sizeMetric := firstMetric(m.after, "apiserver_storage_size_bytes", "apiserver_storage_db_total_size_in_bytes")
	report := &apiserverReport{
		Enabled:              true,
		WindowSeconds:        window.Seconds(),
		InflightMutatingPeak: m.inflightPeak,
		PodsInStorageBefore:  seriesSum(m.before, objectsMetric, `resource="pods"`),
		PodsInStorageAfter:   seriesSum(m.after, objectsMetric, `resource="pods"`),
		DBSizeBytesBefore:    seriesSum(m.before, sizeMetric),
		DBSizeBytesAfter:     seriesSum(m.after, sizeMetric),
		RSSAfterBytes:        seriesSum(m.after, "process_resident_memory_bytes"),
	}
	if window <= 0 {
		return report
	}
	seconds := window.Seconds()
	report.CPUCoresMean = delta(m.before, m.after, "process_cpu_seconds_total") / seconds

	// Every pods create served by this apiserver, regardless of outcome.
	postPods := []string{`verb="POST"`, `resource="pods"`, `group=""`, `version="v1"`, `subresource=""`}
	report.PostRequests = delta(m.before, m.after, "apiserver_request_total", postPods...)
	if report.PostRequests < 0 {
		// Counters only go up, so a negative delta means the load balancer sent
		// the two scrapes to different apiservers; the deltas are meaningless.
		return &apiserverReport{
			Enabled: false,
			Reason:  "before/after scrapes hit different apiserver instances; point KUBECONFIG at one control plane to get valid deltas",
		}
	}
	report.PostRatePerSecond = report.PostRequests / seconds
	report.Code201 = delta(m.before, m.after, "apiserver_request_total", append(postPods, `code="201"`)...)
	report.Code429 = delta(m.before, m.after, "apiserver_request_total", append(postPods, `code="429"`)...)
	// Codes are three digits, so this matches 500 through 599 and nothing else.
	report.Code5xx = delta(m.before, m.after, "apiserver_request_total", append(postPods, `code="5`)...)
	report.PostMeanMs = 1000 * meanOfDelta(m.before, m.after,
		"apiserver_request_duration_seconds_sum", "apiserver_request_duration_seconds_count", postPods...)
	report.EtcdCreateMeanMs = 1000 * meanOfDelta(m.before, m.after,
		"etcd_request_duration_seconds_sum", "etcd_request_duration_seconds_count",
		`operation="create"`, `resource="pods"`)
	if report.EtcdCreateMeanMs == 0 {
		// Older apiservers label the etcd metric with the Go type instead.
		report.EtcdCreateMeanMs = 1000 * meanOfDelta(m.before, m.after,
			"etcd_request_duration_seconds_sum", "etcd_request_duration_seconds_count",
			`operation="create"`, `type="pods"`)
	}
	report.WebhookAdmitMeanMs = 1000 * meanOfDelta(m.before, m.after,
		"apiserver_admission_webhook_admission_duration_seconds_sum",
		"apiserver_admission_webhook_admission_duration_seconds_count", `type="admit"`)
	report.APFRejected = delta(m.before, m.after, "apiserver_flowcontrol_rejected_requests_total")
	report.AllRequestsPerSecond = delta(m.before, m.after, "apiserver_request_total") / seconds
	return report
}

func (m *apiserverMetrics) scrape() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	raw, err := m.client.Discovery().RESTClient().Get().AbsPath("/metrics").DoRaw(ctx)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func delta(before, after, name string, labels ...string) float64 {
	return seriesSum(after, name, labels...) - seriesSum(before, name, labels...)
}

func meanOfDelta(before, after, sumName, countName string, labels ...string) float64 {
	count := delta(before, after, countName, labels...)
	if count <= 0 {
		return 0
	}
	return delta(before, after, sumName, labels...) / count
}

// firstMetric returns the first of the candidate metric names present in the
// exposition. Kubernetes renamed several apiserver metrics across versions and
// only one name is ever served, so the report works on old and new clusters.
func firstMetric(text string, names ...string) string {
	for _, name := range names {
		if hasMetric(text, name) {
			return name
		}
	}
	return names[0]
}

func hasMetric(text, name string) bool {
	prefix := name + "{"
	for _, line := range strings.Split(text, "\n") {
		if line == "" || line[0] == '#' {
			continue
		}
		if strings.HasPrefix(line, prefix) || strings.HasPrefix(line, name+" ") {
			return true
		}
	}
	return false
}

// seriesSum adds up every sample of the named metric whose label set contains
// all of the given label substrings. Naive matching keeps this short; the
// exposition format's label order is fixed per metric family.
func seriesSum(text, name string, labels ...string) float64 {
	var total float64
	for _, line := range strings.Split(text, "\n") {
		if line == "" || line[0] == '#' {
			continue
		}
		space := strings.LastIndexByte(line, ' ')
		if space < 0 {
			continue
		}
		series, value := line[:space], line[space+1:]
		seriesName, seriesLabels := series, ""
		if brace := strings.IndexByte(series, '{'); brace >= 0 {
			seriesName = series[:brace]
			if end := strings.LastIndexByte(series, '}'); end > brace {
				seriesLabels = series[brace+1 : end]
			}
		}
		if seriesName != name {
			continue
		}
		matched := true
		for _, label := range labels {
			if !strings.Contains(seriesLabels, label) {
				matched = false
				break
			}
		}
		if !matched {
			continue
		}
		parsed, err := strconv.ParseFloat(value, 64)
		if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
			continue
		}
		total += parsed
	}
	return total
}
