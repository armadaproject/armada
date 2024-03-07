package quarantine

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/scheduler/failureestimator"
)

// QueueQuarantiner determines whether queues should be quarantined,
// i.e., whether we should reduce the rate which we schedule jobs from the queue,
// based on the estimated failure probability of the queue.
//
// Specifically, each queue has a quarantine factor associated with it equal to:
// - Zero, if the failure probability estimate was last updated more then failureProbabilityEstimateTimeout ago.
// - Failure probability estimate of the queue multiplied by quarantineFactorMultiplier otherwise.
type QueueQuarantiner struct {
	// Multiply the failure probability by this value to produce the qurantineFactor.
	quarantineFactorMultiplier float64
	// Ignore failure probability estimates with no updates for at least this amount of time.
	failureProbabilityEstimateTimeout time.Duration
	// Provides failure probability estimates.
	failureEstimator *failureestimator.FailureEstimator

	// Prometheus metrics.
	isQuarantinedDesc *prometheus.Desc
}

func NewQueueQuarantiner(
	quarantineFactorMultiplier float64,
	failureProbabilityEstimateTimeout time.Duration,
	failureEstimator *failureestimator.FailureEstimator,
) (*QueueQuarantiner, error) {
	if quarantineFactorMultiplier < 0 || quarantineFactorMultiplier > 1 {
		return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "quarantineFactorMultiplier",
			Value:   quarantineFactorMultiplier,
			Message: fmt.Sprintf("outside allowed range [0, 1]"),
		})
	}
	if failureProbabilityEstimateTimeout < 0 {
		return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "failureProbabilityEstimateTimeout",
			Value:   failureProbabilityEstimateTimeout,
			Message: fmt.Sprintf("outside allowed range [0, Inf)"),
		})
	}
	return &QueueQuarantiner{
		quarantineFactorMultiplier:        quarantineFactorMultiplier,
		failureProbabilityEstimateTimeout: failureProbabilityEstimateTimeout,
		failureEstimator:                  failureEstimator,
		isQuarantinedDesc: prometheus.NewDesc(
			fmt.Sprintf("%s_%s_queue_quarantined", namespace, subsystem),
			"Indicates which queues are quarantined and for what reason.",
			[]string{"queue", "reason"},
			nil,
		),
	}, nil
}

// QuarantineFactor returns a value in [0, 1] indicating to which extent the queue should be quarantined,
// where 0.0 indicates not at all and 1.0 completely.
func (qq *QueueQuarantiner) QuarantineFactor(t time.Time, queueName string) float64 {
	if qq.failureEstimator.IsDisabled() {
		return 0
	}
	failureProbability, timeOfLastUpdate, ok := qq.failureEstimator.FailureProbabilityFromQueueName(queueName)
	if !ok {
		// No estimate available for this node.
		return 0
	}
	return qq.quarantineFactor(t, failureProbability, timeOfLastUpdate)
}

func (qq *QueueQuarantiner) quarantineFactor(t time.Time, failureProbability float64, timeOfLastUpdate time.Time) float64 {
	if t.Sub(timeOfLastUpdate) > qq.failureProbabilityEstimateTimeout {
		// Failure probability estimate hasn't been updated recently.
		return 0
	}
	return failureProbability * qq.quarantineFactorMultiplier
}

func (qq *QueueQuarantiner) Describe(ch chan<- *prometheus.Desc) {
	ch <- qq.isQuarantinedDesc
}

func (qq *QueueQuarantiner) Collect(ch chan<- prometheus.Metric) {
	if qq.failureEstimator.IsDisabled() {
		return
	}
	t := time.Now()
	qq.failureEstimator.ApplyQueues(func(queueName string, failureProbability float64, timeOfLastUpdate time.Time) {
		ch <- prometheus.MustNewConstMetric(
			qq.isQuarantinedDesc, prometheus.GaugeValue,
			qq.quarantineFactor(t, failureProbability, timeOfLastUpdate),
			queueName,
			highFailureProbabilityQuarantineReason,
		)
	})
}
