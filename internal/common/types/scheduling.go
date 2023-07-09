package types

type PriorityClass struct {
	Priority int32
	// If true, Armada may preempt jobs of this class to improve fairness.
	Preemptible bool
	// Limits resources assigned to jobs of this priority class.
	// Specifically, jobs of this priority class are only scheduled if doing so does not exceed this limit.
	MaximumResourceFractionPerQueue map[string]float64
	// Per-pool override of MaximumResourceFractionPerQueue.
	// If missing for a particular pool, MaximumResourceFractionPerQueue is used instead for that pool.
	MaximumResourceFractionPerQueueByPool map[string]map[string]float64
}
