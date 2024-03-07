package failureestimator

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"gonum.org/v1/gonum/mat"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/linalg"
	armadamath "github.com/armadaproject/armada/internal/common/math"
	"github.com/armadaproject/armada/internal/common/optimisation"
	"github.com/armadaproject/armada/internal/common/slices"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
)

const (
	namespace = "armada"
	subsystem = "scheduler"

	// Floating point tolerance. Also used when applying limits to avoid divide-by-zero issues.
	eps = 1e-15
)

// FailureEstimator is a system for answering the following question:
// "What's the probability of a job from queue Q completing successfully when scheduled on node N?"
// We assume the job may fail either because the job or node is faulty, and we assume these failures are independent.
// Denote by
// - P_q the probability of a job from queue q succeeding when running on a perfect node and
// - P_n is the probability of a perfect job succeeding on node n.
// The success probability of a job from queue q on node n is then Pr(p_q*p_n = 1),
// where p_q and p_n are drawn from Bernoulli distributions with parameter P_q and P_n, respectively.
//
// Now, the goal is to jointly estimate P_q and P_n for each queue and node from observed successes and failures.
// We do so here with a statistical method. The intuition of the method is that:
// - A job from a queue with many failures failing doesn't say much about the node; likely the problem is with the job.
// - A job failing on a node with many failures doesn't say much about the job; likely the problem is with the node.
// And vice versa.
//
// Specifically, we maximise the log-likelihood function of P_q and P_n over observed successes and failures.
// This maximisation is performed using online gradient descent, where for each success or failure,
// we update the corresponding P_q and P_n by taking a gradient step. See the Update() function for details.
//
// This module internally only maintains success probability estimates, as this makes the maths cleaner.
// We convert these to failure probabilities when exporting these via API calls.
type FailureEstimator struct {
	// Success probability estimates for all nodes and queues.
	parameters             *mat.VecDense
	intermediateParameters *mat.VecDense

	// Gradient buffer.
	gradient *mat.VecDense

	// Maps node (queue) names to the corresponding index of parameters.
	// E.g., if nodeByName["myNode"].parameterIndex = 10, then parameters[10] is the estimated success probability of myNode.
	nodeByName  map[string]node
	queueByName map[string]queue

	// Samples that have not been processed yet.
	samples []sample

	// Optimisation settings.
	numInnerIterations int
	innerOptimiser     optimisation.Optimiser
	outerOptimiser     optimisation.Optimiser

	// Prometheus metrics.
	failureProbabilityByNodeDesc  *prometheus.Desc
	failureProbabilityByQueueDesc *prometheus.Desc

	// If true, this module is disabled.
	disabled bool

	// Mutex protecting the above fields.
	// Prevents concurrent map modification when scraping metrics.
	mu sync.Mutex
}

type node struct {
	parameterIndex         int
	cluster                string
	timeOfMostRecentSample time.Time
}

type queue struct {
	parameterIndex         int
	timeOfMostRecentSample time.Time
}

type sample struct {
	i int
	j int
	c bool
}

// New returns a new FailureEstimator.
func New(
	numInnerIterations int,
	innerOptimiser optimisation.Optimiser,
	outerOptimiser optimisation.Optimiser,
) (*FailureEstimator, error) {
	if numInnerIterations < 1 {
		return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "numInnerIterations",
			Value:   numInnerIterations,
			Message: fmt.Sprintf("outside allowed range [1, Inf)"),
		})
	}
	return &FailureEstimator{
		parameters:             mat.NewVecDense(32, armadaslices.Fill[float64](0.5, 32)),
		intermediateParameters: mat.NewVecDense(32, armadaslices.Zeros[float64](32)),
		gradient:               mat.NewVecDense(32, armadaslices.Zeros[float64](32)),

		nodeByName:  make(map[string]node, 16),
		queueByName: make(map[string]queue, 16),

		numInnerIterations: numInnerIterations,
		innerOptimiser:     innerOptimiser,
		outerOptimiser:     outerOptimiser,

		failureProbabilityByNodeDesc: prometheus.NewDesc(
			fmt.Sprintf("%s_%s_node_failure_probability", namespace, subsystem),
			"Estimated per-node failure probability.",
			[]string{"node", "cluster"},
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

// Push adds a sample to the internal buffer of the failure estimator.
// Samples added via Push are processed on the next call to Update.
// The timestamp t should be the time at which the success or failure happened.
func (fe *FailureEstimator) Push(nodeName, queueName, clusterName string, success bool, t time.Time) {
	fe.mu.Lock()
	defer fe.mu.Unlock()

	node, ok := fe.nodeByName[nodeName]
	if !ok {
		node.parameterIndex = len(fe.nodeByName) + len(fe.queueByName)
	}
	node.cluster = clusterName
	if node.timeOfMostRecentSample.Compare(t) == -1 {
		node.timeOfMostRecentSample = t
	}
	fe.nodeByName[nodeName] = node

	queue, ok := fe.queueByName[queueName]
	if !ok {
		queue.parameterIndex = len(fe.nodeByName) + len(fe.queueByName)
	}
	if queue.timeOfMostRecentSample.Compare(t) == -1 {
		queue.timeOfMostRecentSample = t
	}
	fe.queueByName[queueName] = queue

	fe.extendParameters(armadamath.Max(node.parameterIndex, queue.parameterIndex) + 1)
	fe.samples = append(fe.samples, sample{
		i: node.parameterIndex,
		j: queue.parameterIndex,
		c: success,
	})
}

func (fe *FailureEstimator) extendParameters(n int) {
	oldN := fe.parameters.Len()
	fe.parameters = linalg.ExtendVecDense(fe.parameters, n)
	if oldN < n {
		for i := oldN; i < n; i++ {
			// Initialise new parameters with 50% success probability.
			fe.parameters.SetVec(i, 0.5)
		}
	}
	fe.intermediateParameters = linalg.ExtendVecDense(fe.intermediateParameters, n)
	fe.gradient = linalg.ExtendVecDense(fe.gradient, n)
}

// Update success probability estimates based on pushed samples.
func (fe *FailureEstimator) Update() {
	fe.mu.Lock()
	defer fe.mu.Unlock()
	if len(fe.samples) == 0 {
		// Nothing to do.
		return
	}

	// Inner loop to compute intermediateParameters from samples.
	// Passing over samples multiple times in random order helps improve convergence.
	fe.intermediateParameters.CopyVec(fe.parameters)
	for k := 0; k < fe.numInnerIterations; k++ {

		// Compute gradient with respect to updates.
		fe.gradient.Zero()
		slices.Shuffle(fe.samples)
		for _, sample := range fe.samples {
			gi, gj := fe.negLogLikelihoodGradient(
				fe.intermediateParameters.AtVec(sample.i),
				fe.intermediateParameters.AtVec(sample.j),
				sample.c,
			)
			fe.gradient.SetVec(sample.i, fe.gradient.AtVec(sample.i)+gi)
			fe.gradient.SetVec(sample.j, fe.gradient.AtVec(sample.j)+gj)
		}

		// Update intermediateParameters using this gradient.
		fe.innerOptimiser.Extend(fe.intermediateParameters.Len())
		fe.intermediateParameters = fe.innerOptimiser.Update(fe.intermediateParameters, fe.intermediateParameters, fe.gradient)
		applyBoundsVec(fe.intermediateParameters)
	}

	// Let the gradient be the difference between parameters and intermediateParameters,
	// i.e., we use the inner loop as a method to estimate the gradient,
	// and then update parameters using this gradient and the outer optimiser.
	fe.gradient.CopyVec(fe.parameters)
	fe.gradient.SubVec(fe.gradient, fe.intermediateParameters)
	fe.outerOptimiser.Extend(fe.parameters.Len())
	fe.parameters = fe.outerOptimiser.Update(fe.parameters, fe.parameters, fe.gradient)
	applyBoundsVec(fe.parameters)

	// Empty the buffer.
	fe.samples = fe.samples[0:0]
}

func applyBoundsVec(vec *mat.VecDense) {
	for i := 0; i < vec.Len(); i++ {
		vec.SetVec(i, applyBounds(vec.AtVec(i)))
	}
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

// negLogLikelihoodGradient returns the gradient of the negated log-likelihood function.
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

// FailureProbabilityFromNodeName returns the failure probability estimate of the named node
// and the timestamp of the most recent success or failure observed for this node.
// The most recent sample may not be reflected in the estimate if Update has not been called since the last call to Push.
// If there is no estimate for nodeName, the final return value is false.
func (fe *FailureEstimator) FailureProbabilityFromNodeName(nodeName string) (float64, time.Time, bool) {
	node, ok := fe.nodeByName[nodeName]
	if !ok {
		return 0, time.Time{}, false
	}
	return 1 - fe.parameters.AtVec(node.parameterIndex), node.timeOfMostRecentSample, true
}

// FailureProbabilityFromQueueName returns the failure probability estimate of the named queue
// and the timestamp of the most recent success or failure observed for this queue.
// The most recent sample may not be reflected in the estimate if Update has not been called since the last call to Push.
// If there is no estimate for queueName, the final return value is false.
func (fe *FailureEstimator) FailureProbabilityFromQueueName(queueName string) (float64, time.Time, bool) {
	queue, ok := fe.nodeByName[queueName]
	if !ok {
		return 0, time.Time{}, false
	}
	return 1 - fe.parameters.AtVec(queue.parameterIndex), queue.timeOfMostRecentSample, true
}

func (fe *FailureEstimator) ApplyNodes(f func(nodeName, cluster string, failureProbability float64, timeOfLastUpdate time.Time)) {
	fe.mu.Lock()
	defer fe.mu.Unlock()
	for nodeName, node := range fe.nodeByName {
		// Report failure probability rounded to nearest multiple of 0.01.
		// (As it's unlikely the estimate is accurate to within less than this.)
		failureProbability := 1 - fe.parameters.AtVec(node.parameterIndex)
		failureProbability = math.Round(failureProbability*100) / 100
		f(nodeName, node.cluster, failureProbability, node.timeOfMostRecentSample)
	}
}

func (fe *FailureEstimator) ApplyQueues(f func(queueName string, failureProbability float64, timeOfLastUpdate time.Time)) {
	fe.mu.Lock()
	defer fe.mu.Unlock()
	for queueName, queue := range fe.queueByName {
		// Report failure probability rounded to nearest multiple of 0.01.
		// (As it's unlikely the estimate is accurate to within less than this.)
		failureProbability := 1 - fe.parameters.AtVec(queue.parameterIndex)
		failureProbability = math.Round(failureProbability*100) / 100
		f(queueName, failureProbability, queue.timeOfMostRecentSample)
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
	fe.ApplyNodes(func(nodeName, cluster string, failureProbability float64, timeOfLastUpdate time.Time) {
		ch <- prometheus.MustNewConstMetric(fe.failureProbabilityByNodeDesc, prometheus.GaugeValue, failureProbability, nodeName, cluster)
	})
	fe.ApplyQueues(func(queueName string, failureProbability float64, timeOfLastUpdate time.Time) {
		ch <- prometheus.MustNewConstMetric(fe.failureProbabilityByQueueDesc, prometheus.GaugeValue, failureProbability, queueName)
	})
}
