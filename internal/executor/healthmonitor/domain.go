package healthmonitor

import "time"

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
		lastCheck: time.Unix(e.lastCheck.Unix(), int64(e.lastCheck.Nanosecond())),
		err:       e.err,
		metrics:   metricsCopy,
	}
}

type FakeEtcdLimitHealthMonitor struct {
	IsWithinSoftLimit bool
	IsWithinHardLimit bool
}

func (f *FakeEtcdLimitHealthMonitor) IsWithinSoftHealthLimit() bool {
	return f.IsWithinSoftLimit
}

func (f *FakeEtcdLimitHealthMonitor) IsWithinHardHealthLimit() bool {
	return f.IsWithinHardLimit
}
