package metrics

import "github.com/armadaproject/armada/pkg/metricevents"

type MetricsPublisher interface {
	Publish([]*metricevents.Event) error
}
