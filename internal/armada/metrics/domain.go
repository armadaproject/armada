package metrics

type QueueMetrics struct {
	Resources map[string]ResourceMetrics
	Durations map[string]*FloatMetrics
}
