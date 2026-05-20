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

// zombiesSkippedNullFinished counts zombie jobs the pruner saw but could not
// repair because their latest run row had a NULL finished timestamp. The
// reconciler relies on finished to populate last_transition_time, so these
// rows are left alone -- but they should not exist in steady state and are
// worth surfacing as their own signal so the gap is observable.
var zombiesSkippedNullFinished = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "lookout_pruner_zombie_jobs_skipped_null_finished_total",
		Help: "Number of zombie jobs skipped by the lookout pruner because the latest run had no finished timestamp.",
	},
)
