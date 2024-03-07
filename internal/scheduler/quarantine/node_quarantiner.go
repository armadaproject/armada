package quarantine

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/scheduler/failureestimator"
)

const (
	namespace = "armada"
	subsystem = "scheduler"

	highFailureProbabilityQuarantineReason = "highFailureProbability"
)

var highFailureProbabilityTaint = v1.Taint{
	Key:    "armadaproject.io/schedulerInternal/quarantined",
	Value:  highFailureProbabilityQuarantineReason,
	Effect: v1.TaintEffectNoSchedule,
}

// NodeQuarantiner determines whether nodes should be quarantined,
// i.e., removed from consideration when scheduling new jobs,
// based on the estimated failure probability of the node.
//
// Specifically, any node for which the following is true is quarantined:
// 1. The estimated failure probability exceeds failureProbabilityQuarantineThreshold.
// 2. The failure probability estimate was updated at most failureProbabilityEstimateTimeout ago.
type NodeQuarantiner struct {
	// Quarantine nodes with a failure probability greater than this threshold.
	failureProbabilityQuarantineThreshold float64
	// Ignore failure probability estimates with no updates for at least this amount of time.
	failureProbabilityEstimateTimeout time.Duration
	// Provides failure probability estimates.
	failureEstimator *failureestimator.FailureEstimator

	// Prometheus metrics.
	isQuarantinedDesc *prometheus.Desc
}

func NewNodeQuarantiner(
	failureProbabilityQuarantineThreshold float64,
	failureProbabilityEstimateTimeout time.Duration,
	failureEstimator *failureestimator.FailureEstimator,
) (*NodeQuarantiner, error) {
	if failureProbabilityQuarantineThreshold < 0 || failureProbabilityQuarantineThreshold > 1 {
		return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "failureProbabilityQuarantineThreshold",
			Value:   failureProbabilityQuarantineThreshold,
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
	return &NodeQuarantiner{
		failureProbabilityQuarantineThreshold: failureProbabilityQuarantineThreshold,
		failureProbabilityEstimateTimeout:     failureProbabilityEstimateTimeout,
		failureEstimator:                      failureEstimator,
		isQuarantinedDesc: prometheus.NewDesc(
			fmt.Sprintf("%s_%s_node_quarantined", namespace, subsystem),
			"Indicates which nodes are quarantined and for what reason.",
			[]string{"node", "cluster", "reason"},
			nil,
		),
	}, nil
}

// IsQuarantined returns true if the node is quarantined and a taint expressing the reason why, and false otherwise.
func (nq *NodeQuarantiner) IsQuarantined(t time.Time, nodeName string) (taint v1.Taint, isQuarantined bool) {
	if nq.failureEstimator.IsDisabled() {
		return
	}
	failureProbability, timeOfLastUpdate, ok := nq.failureEstimator.FailureProbabilityFromNodeName(nodeName)
	if !ok {
		// No estimate available for this node.
		return
	}
	if !nq.isQuarantined(t, failureProbability, timeOfLastUpdate) {
		return
	}
	return highFailureProbabilityTaint, true
}

func (nq *NodeQuarantiner) isQuarantined(t time.Time, failureProbability float64, timeOfLastUpdate time.Time) bool {
	if failureProbability < nq.failureProbabilityQuarantineThreshold {
		// Failure probability does not exceed threshold.
		return false
	}
	if t.Sub(timeOfLastUpdate) > nq.failureProbabilityEstimateTimeout {
		// Failure probability estimate hasn't been updated recently.
		return false
	}
	return true
}

func (nq *NodeQuarantiner) Describe(ch chan<- *prometheus.Desc) {
	ch <- nq.isQuarantinedDesc
}

func (nq *NodeQuarantiner) Collect(ch chan<- prometheus.Metric) {
	if nq.failureEstimator.IsDisabled() {
		return
	}
	t := time.Now()
	nq.failureEstimator.ApplyNodes(func(nodeName, cluster string, failureProbability float64, timeOfLastUpdate time.Time) {
		v := 0.0
		if nq.isQuarantined(t, failureProbability, timeOfLastUpdate) {
			v = 1.0
		}
		ch <- prometheus.MustNewConstMetric(nq.isQuarantinedDesc, prometheus.GaugeValue, v, nodeName, cluster, highFailureProbabilityQuarantineReason)
	})
}
