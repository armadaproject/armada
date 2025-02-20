package priorityoverride

type overrideKey struct {
	pool  string
	queue string
}

// Provider provides per-pool-per-queue priority overrides
// These can be used to override the priority of a given queue
type Provider interface {
	Ready() bool
	Override(pool, queue string) (float64, bool, error)
}
