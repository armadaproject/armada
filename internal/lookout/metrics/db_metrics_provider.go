package metrics

type LookoutDbMetricsProvider interface {
	GetOpenConnections()
}
