package pruner

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// zombiesRepaired counts the number of zombie jobs (non-terminal job.state but
// terminal latest run) that the pruner has repaired.
var zombiesRepaired = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "lookout_pruner_zombie_jobs_repaired_total",
		Help: "Number of zombie jobs (non-terminal job.state but terminal latest run) repaired by the lookout pruner.",
	},
)
