package priorityoverride

type overrideKey struct {
	pool  string
	queue string
}

// Provider provides per-pool, per-queue overrides of a queue's priority factor.
// A positive override replaces the factor used to compute the queue's fair-share weight.
type Provider interface {
	Ready() bool
	Override(pool, queue string) (float64, bool, error)
}
