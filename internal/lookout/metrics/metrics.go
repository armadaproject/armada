package metrics

import (
	"math"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/database"
	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/lookout/schema"
)

const jobTablePartitionedTimeout = 5 * time.Second

var requestDuration = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "lookout_request_duration_ms",
		Help:    "Request duration in milliseconds",
		Buckets: []float64{1, 10, 100, 1000, 10000, 100000, 1000000},
	},
	[]string{"user", "endpoint"},
)

func RecordRequestDuration(user, endpoint string, duration float64) {
	requestDuration.
		With(map[string]string{"user": user, "endpoint": endpoint}).
		Observe(duration)
}

// RegisterJobTablePartitioned registers the lookout_job_table_partitioned
// gauge, which checks db on every scrape so that it reflects a migration
// without a restart.
func RegisterJobTablePartitioned(db database.Querier) {
	prometheus.MustRegister(newJobTablePartitionedGauge(db))
}

func newJobTablePartitionedGauge(db database.Querier) prometheus.GaugeFunc {
	return prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "lookout_job_table_partitioned",
			Help: "1 if the job table is partitioned, 0 if it uses the legacy unpartitioned schema",
		},
		func() float64 {
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), jobTablePartitionedTimeout)
			defer cancel()
			partitioned, err := schema.IsJobTablePartitioned(ctx, db)
			if err != nil {
				log.WithError(err).Warn("Failed to check whether the job table is partitioned")
				return math.NaN()
			}
			if partitioned {
				return 1
			}
			return 0
		},
	)
}
