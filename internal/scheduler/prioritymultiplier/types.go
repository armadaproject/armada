package prioritymultiplier

type multiplierKey struct {
	pool  string
	queue string
}

// Provider provides per-pool-per-queue priority multipliers
// These can be used to increase the priority of a given queue over its base level
type Provider interface {
	Ready() bool
	Multiplier(pool, queue string) (float64, error)
}
