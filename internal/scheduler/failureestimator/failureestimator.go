package failureestimator

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
)

const (
	namespace = "armada"
	subsystem = "scheduler"

	// Floating point tolerance. Also used when applying limits to avoid divide-by-zero issues.
	eps = 1e-15
	// Assumed success probability of "good" nodes (queues) used when calculating step size.
	healthySuccessProbability = 0.95
)

// FailureEstimator is a system for answering the following question:
// "What's the probability of a job from queue Q completing successfully when scheduled on node N?"
// We assume the job may fail either because the job or node is faulty, and we assume these failures are independent.
// Denote by
// - P_q the probability of a job from queue q succeeding when running on a perfect node and
// - P_n is the probability of a perfect job succeeding on node n.
// The success probability of a job from queue q on node n is then Pr(p_q*p_n=1),
// where p_q and p_n are drawn from Bernoulli distributions with parameter P_q and P_n, respectively.
//
// Now, the goal is to jointly estimate P_q and P_n for each queue and node using observed successes and failures.
// The method used is statistical only relies on knowing which queue a job belongs to and on which node it ran.
// Specifically, we maximise the log-likelihood function of P_q and P_n using observed successes and failures.
// This maximisation is performed using online gradient descent, where for each success or failure,
// we update the corresponding P_q and P_n by taking a gradient step.
// See New(...) for more details regarding step size.
//
// Finally, we exponentially decay P_q and P_N towards 1 over time,
// such that nodes and queues for which we observe no failures appear to become healthier over time.
// See New(...) function for details regarding decay.
//
// This module internally only maintains success probability estimates, as this makes the maths cleaner.
// When exporting these via API calls we convert to failure probabilities as these are more intuitive to reason about.
type FailureEstimator struct {
	// Map from node (queue) name to the estimated success probability of that node (queue). For example:
	// - successProbabilityByNode["myNode"] = 0.85]: estimated failure probability of a perfect job run on "myNode" is 15%.
	// - successProbabilityByQueue["myQueue"] = 0.60]: estimated failure probability of a job from "myQueue" run on a perfect node is 40%.
	successProbabilityByNode  map[string]float64
	successProbabilityByQueue map[string]float64

	// Success probability below which to consider nodes (jobs) unhealthy.
	nodeSuccessProbabilityCordonThreshold  float64
	queueSuccessProbabilityCordonThreshold float64

	// Controls how quickly estimated node (queue) success probability increase in absence of any evidence.
	// Computed from {node, queue}SuccessProbabilityCordonThreshold and {node, queue}CordonTimeout.
	nodeFailureProbabilityDecayRate  float64
	queueFailureProbabilityDecayRate float64
	timeOfLastDecay                  time.Time

	// Gradient descent step size. Controls the extent to which new data affects successProbabilityBy{Node, Queue}.
	// Computed from {node, queue}SuccessProbabilityCordonThreshold, {node, queue}FailureProbabilityDecayRate, {node, queue}EquilibriumFailureRate.
	nodeStepSize  float64
	queueStepSize float64

	// Prometheus metrics.
	failureProbabilityByNodeDesc  *prometheus.Desc
	failureProbabilityByQueueDesc *prometheus.Desc

	// If true, this module is disabled.
	disabled bool

	mu sync.Mutex
}

// New returns a new FailureEstimator. Parameters have the following meaning:
// - {node, queue}SuccessProbabilityCordonThreshold: Success probability below which nodes (queues) are considered unhealthy.
// - {node, queue}CordonTimeout: Amount of time for which nodes (queues) remain unhealthy in the absence of any job successes or failures for that node (queue).
// - {node, queue}EquilibriumFailureRate: Job failures per second necessary for a node (queue) to remain unhealthy in the absence of any successes for that node (queue).
func New(
	nodeSuccessProbabilityCordonThreshold float64,
	queueSuccessProbabilityCordonThreshold float64,
	nodeCordonTimeout time.Duration,
	queueCordonTimeout time.Duration,
	nodeEquilibriumFailureRate float64,
	queueEquilibriumFailureRate float64,
) (*FailureEstimator, error) {
	if nodeSuccessProbabilityCordonThreshold < 0 || nodeSuccessProbabilityCordonThreshold > 1 {
		return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "nodeSuccessProbabilityCordonThreshold",
			Value:   nodeSuccessProbabilityCordonThreshold,
			Message: fmt.Sprintf("outside allowed range [0, 1]"),
		})
	}
	if queueSuccessProbabilityCordonThreshold < 0 || queueSuccessProbabilityCordonThreshold > 1 {
		return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "queueSuccessProbabilityCordonThreshold",
			Value:   queueSuccessProbabilityCordonThreshold,
			Message: fmt.Sprintf("outside allowed range [0, 1]"),
		})
	}
	if nodeCordonTimeout < 0 {
		return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "nodeCordonTimeout",
			Value:   nodeCordonTimeout,
			Message: fmt.Sprintf("outside allowed range [0, Inf)"),
		})
	}
	if queueCordonTimeout < 0 {
		return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "queueCordonTimeout",
			Value:   queueCordonTimeout,
			Message: fmt.Sprintf("outside allowed range [0, Inf)"),
		})
	}
	if nodeEquilibriumFailureRate <= 0 {
		return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "nodeEquilibriumFailureRate",
			Value:   nodeEquilibriumFailureRate,
			Message: fmt.Sprintf("outside allowed range (0, Inf)"),
		})
	}
	if queueEquilibriumFailureRate <= 0 {
		return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "queueEquilibriumFailureRate",
			Value:   queueEquilibriumFailureRate,
			Message: fmt.Sprintf("outside allowed range (0, Inf)"),
		})
	}

	// Compute decay rates such that a node (queue) with success probability 0 will over {node, queue}CordonTimeout time
	// decay to a success probability of {node, queue}SuccessProbabilityCordonThreshold.
	nodeFailureProbabilityDecayRate := math.Exp(math.Log(1-nodeSuccessProbabilityCordonThreshold) / nodeCordonTimeout.Seconds())
	queueFailureProbabilityDecayRate := math.Exp(math.Log(1-queueSuccessProbabilityCordonThreshold) / queueCordonTimeout.Seconds())

	// Compute step size such that a node (queue) with success probability {node, queue}SuccessProbabilityCordonThreshold
	// for which we observe 0 successes and {node, queue}EquilibriumFailureRate failures per second from "good" nodes (queues)
	// will remain at exactly {node, queue}SuccessProbabilityCordonThreshold success probability.
	dNodeSuccessProbability := healthySuccessProbability / (1 - nodeSuccessProbabilityCordonThreshold*healthySuccessProbability)
	dQueueSuccessProbability := healthySuccessProbability / (1 - queueSuccessProbabilityCordonThreshold*healthySuccessProbability)
	nodeStepSize := (1 - nodeSuccessProbabilityCordonThreshold - (1-nodeSuccessProbabilityCordonThreshold)*nodeFailureProbabilityDecayRate) / dNodeSuccessProbability / nodeEquilibriumFailureRate
	queueStepSize := (1 - queueSuccessProbabilityCordonThreshold - (1-queueSuccessProbabilityCordonThreshold)*queueFailureProbabilityDecayRate) / dQueueSuccessProbability / queueEquilibriumFailureRate

	return &FailureEstimator{
		successProbabilityByNode:               make(map[string]float64, 1024),
		successProbabilityByQueue:              make(map[string]float64, 128),
		nodeSuccessProbabilityCordonThreshold:  nodeSuccessProbabilityCordonThreshold,
		queueSuccessProbabilityCordonThreshold: queueSuccessProbabilityCordonThreshold,
		nodeFailureProbabilityDecayRate:        nodeFailureProbabilityDecayRate,
		queueFailureProbabilityDecayRate:       queueFailureProbabilityDecayRate,
		timeOfLastDecay:                        time.Now(),
		nodeStepSize:                           nodeStepSize,
		queueStepSize:                          queueStepSize,
		failureProbabilityByNodeDesc: prometheus.NewDesc(
			fmt.Sprintf("%s_%s_node_failure_probability", namespace, subsystem),
			"Estimated per-node failure probability.",
			[]string{"node"},
			nil,
		),
		failureProbabilityByQueueDesc: prometheus.NewDesc(
			fmt.Sprintf("%s_%s_queue_failure_probability", namespace, subsystem),
			"Estimated per-queue failure probability.",
			[]string{"queue"},
			nil,
		),
	}, nil
}

func (fe *FailureEstimator) Disable(v bool) {
	if fe == nil {
		return
	}
	fe.disabled = v
}

func (fe *FailureEstimator) IsDisabled() bool {
	if fe == nil {
		return true
	}
	return fe.disabled
}

// Decay moves the success probabilities of nodes (queues) closer to 1, depending on the configured cordon timeout.
// Periodically calling Decay() ensures nodes (queues) considered unhealthy are eventually considered healthy again.
func (fe *FailureEstimator) Decay() {
	fe.mu.Lock()
	defer fe.mu.Unlock()
	t := time.Now()
	fe.decay(t.Sub(fe.timeOfLastDecay).Seconds())
	fe.timeOfLastDecay = t
	return
}

func (fe *FailureEstimator) decay(secondsSinceLastDecay float64) {
	nodeFailureProbabilityDecay := math.Pow(fe.nodeFailureProbabilityDecayRate, secondsSinceLastDecay)
	for k, v := range fe.successProbabilityByNode {
		failureProbability := 1 - v
		failureProbability *= nodeFailureProbabilityDecay
		successProbability := 1 - failureProbability
		fe.successProbabilityByNode[k] = applyBounds(successProbability)
	}

	queueFailureProbabilityDecay := math.Pow(fe.queueFailureProbabilityDecayRate, secondsSinceLastDecay)
	for k, v := range fe.successProbabilityByQueue {
		failureProbability := 1 - v
		failureProbability *= queueFailureProbabilityDecay
		successProbability := 1 - failureProbability
		fe.successProbabilityByQueue[k] = applyBounds(successProbability)
	}
	return
}

// Update with success=false decreases the estimated success probability of the provided node and queue.
// Calling with success=true increases the estimated success probability of the provided node and queue.
// This update is performed by taking one gradient descent step.
func (fe *FailureEstimator) Update(node, queue string, success bool) {
	fe.mu.Lock()
	defer fe.mu.Unlock()

	// Assume that nodes (queues) we haven't seen previously are healthy.
	nodeSuccessProbability, ok := fe.successProbabilityByNode[node]
	if !ok {
		nodeSuccessProbability = healthySuccessProbability
	}
	queueSuccessProbability, ok := fe.successProbabilityByQueue[queue]
	if !ok {
		queueSuccessProbability = healthySuccessProbability
	}

	dNodeSuccessProbability, dQueueSuccessProbability := fe.negLogLikelihoodGradient(nodeSuccessProbability, queueSuccessProbability, success)
	nodeSuccessProbability -= fe.nodeStepSize * dNodeSuccessProbability
	queueSuccessProbability -= fe.queueStepSize * dQueueSuccessProbability

	fe.successProbabilityByNode[node] = applyBounds(nodeSuccessProbability)
	fe.successProbabilityByQueue[queue] = applyBounds(queueSuccessProbability)
}

// applyBounds ensures values stay in the range [eps, 1-eps].
// This to avoid divide-by-zero issues.
func applyBounds(v float64) float64 {
	if v < eps {
		return eps
	} else if v > 1.0-eps {
		return 1.0 - eps
	} else {
		return v
	}
}

// negLogLikelihoodGradient returns the gradient of the negated log-likelihood function with respect to P_q and P_n.
func (fe *FailureEstimator) negLogLikelihoodGradient(nodeSuccessProbability, queueSuccessProbability float64, success bool) (float64, float64) {
	if success {
		dNodeSuccessProbability := -1 / nodeSuccessProbability
		dQueueSuccessProbability := -1 / queueSuccessProbability
		return dNodeSuccessProbability, dQueueSuccessProbability
	} else {
		dNodeSuccessProbability := queueSuccessProbability / (1 - nodeSuccessProbability*queueSuccessProbability)
		dQueueSuccessProbability := nodeSuccessProbability / (1 - nodeSuccessProbability*queueSuccessProbability)
		return dNodeSuccessProbability, dQueueSuccessProbability
	}
}

func (fe *FailureEstimator) Describe(ch chan<- *prometheus.Desc) {
	if fe.IsDisabled() {
		return
	}
	ch <- fe.failureProbabilityByNodeDesc
	ch <- fe.failureProbabilityByQueueDesc
}

func (fe *FailureEstimator) Collect(ch chan<- prometheus.Metric) {
	if fe.IsDisabled() {
		return
	}
	fe.mu.Lock()
	defer fe.mu.Unlock()

	// Report failure probability rounded to nearest multiple of 0.01.
	// (As it's unlikely the estimate is accurate to within less than this.)
	for k, v := range fe.successProbabilityByNode {
		failureProbability := 1 - v
		failureProbability = math.Round(failureProbability*100) / 100
		ch <- prometheus.MustNewConstMetric(fe.failureProbabilityByNodeDesc, prometheus.GaugeValue, failureProbability, k)
	}
	for k, v := range fe.successProbabilityByQueue {
		failureProbability := 1 - v
		failureProbability = math.Round(failureProbability*100) / 100
		ch <- prometheus.MustNewConstMetric(fe.failureProbabilityByQueueDesc, prometheus.GaugeValue, failureProbability, k)
	}
}
