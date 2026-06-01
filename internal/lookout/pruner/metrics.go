package pruner

import (
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"

	"github.com/armadaproject/armada/internal/common/armadacontext"
)

// zombiesRepaired records the number of zombie jobs repaired in the current
// pruner run. It is a Gauge rather than a Counter because the pruner pushes to
// a Pushgateway and each push should reflect only the current run, not a
// monotonically increasing total across multiple invocations of the process.
var zombiesRepaired = prometheus.NewGauge(
	prometheus.GaugeOpts{
		Name: "lookout_pruner_zombie_jobs_repaired_total",
		Help: "Number of zombie jobs (non-terminal job.state but terminal latest run) repaired by the lookout pruner in the most recent run.",
	},
)

// zombiesSkippedNullFinished is the most recently observed count of zombie
// jobs the pruner saw but could not repair because their latest run row had a
// NULL finished timestamp. The reconciler relies on finished to populate
// last_transition_time, so these rows are left alone -- but they should not
// exist in steady state and are worth surfacing as their own signal so the
// gap is observable.
//
// This is a gauge rather than a counter because the underlying population is
// what we care about: the same row staying unrepaired across N runs should
// register as "1 zombie", not "N zombies". A counter would re-count the same
// rows on every run.
var zombiesSkippedNullFinished = prometheus.NewGauge(
	prometheus.GaugeOpts{
		Name: "lookout_pruner_zombie_jobs_skipped_null_finished",
		Help: "Most recent count of zombie jobs skipped by the lookout pruner because the latest run had no finished timestamp.",
	},
)

// PushMetrics pushes all pruner metrics to the Prometheus Pushgateway at url,
// labelled with the given job name. A fresh registry is created on every call
// so that each push reflects only the current run's metric values and the
// package-level metric vars are not permanently bound to a single registry.
func PushMetrics(ctx *armadacontext.Context, url, jobName string) error {
	reg := prometheus.NewRegistry()
	reg.MustRegister(zombiesRepaired, zombiesSkippedNullFinished)

	pusher := push.New(url, jobName).Gatherer(reg)
	if err := pusher.PushContext(ctx); err != nil {
		return errors.WithStack(err)
	}
	return nil
}
