package retry

import (
	"fmt"

	"github.com/armadaproject/armada/pkg/armadaevents"
)

const (
	reasonMatchFail        = "matched rule: Fail"
	reasonMatchRetry       = "matched rule: Retry"
	reasonDefault          = "no rule matched, using default action"
	reasonRetriesDisabled  = "global max retries is 0, retries disabled"
	reasonNoErrorAvailable = "no error information available"
)

// Engine evaluates retry policies against job run errors to decide whether
// a job should be retried or permanently failed.
type Engine struct {
	globalMaxRetries uint
}

// NewEngine creates a retry engine with the given hard upper limit on retries.
func NewEngine(globalMaxRetries uint) *Engine {
	return &Engine{globalMaxRetries: globalMaxRetries}
}

// Counts holds the per-job tallies the engine needs to enforce retry limits.
type Counts struct {
	// Failures is the number of genuinely failed runs, including the run being
	// evaluated. Preempted and lease-returned runs are not failures the job
	// caused; they are excluded upstream, so they never consume a retry budget.
	Failures uint32
}

// Evaluate applies the policy rules to runError and returns a retry decision.
//
// Checks run in a fixed order: missing run error, global cap, rule match,
// then the per-policy limit.
//
// Both limits count retries, not attempts: RetryLimit=3 allows 3 retries after
// the initial failure, i.e. 4 attempts total. RetryLimit=0 means the policy
// never retries. globalMaxRetries=0 disables all retries (kill switch).
//
// policy must not be nil. runError may be nil (treated as "no decision").
func (e *Engine) Evaluate(policy *Policy, runError *armadaevents.Error, counts Counts) Result {
	if runError == nil {
		return Result{ShouldRetry: false, Reason: reasonNoErrorAvailable, Decision: DecisionNoError}
	}

	if e.globalMaxRetries == 0 {
		return Result{ShouldRetry: false, Reason: reasonRetriesDisabled, Decision: DecisionFailGlobalLimit}
	}

	retriesUsed := uint(0)
	if counts.Failures > 0 {
		retriesUsed = uint(counts.Failures) - 1
	}
	if retriesUsed >= e.globalMaxRetries {
		return Result{
			ShouldRetry: false,
			Reason:      fmt.Sprintf("global max retries exceeded (%d/%d)", retriesUsed, e.globalMaxRetries),
			Decision:    DecisionFailGlobalLimit,
		}
	}

	matched := matchRules(policy.Rules, matchInput{
		category:    runError.GetFailureCategory(),
		subcategory: runError.GetFailureSubcategory(),
	})

	action, reason := policy.DefaultAction, reasonDefault
	if matched != nil {
		action = matched.Action
		reason = reasonMatchRetry
		if action == ActionFail {
			reason = reasonMatchFail
		}
	}

	if action == ActionFail {
		decision := DecisionFailDefault
		if matched != nil {
			decision = DecisionFailRule
		}
		return Result{ShouldRetry: false, Reason: reason, Decision: decision}
	}

	if retriesUsed >= uint(policy.RetryLimit) {
		return Result{
			ShouldRetry: false,
			Reason:      fmt.Sprintf("policy retry limit exceeded (%d/%d)", retriesUsed, policy.RetryLimit),
			Decision:    DecisionFailPolicyLimit,
		}
	}

	// The retry carries the matched rule's mutation, if any. A default-action
	// retry (no rule matched) applies no mutation.
	mutation := Mutation{}
	if matched != nil {
		mutation = matched.Mutation
	}
	return Result{ShouldRetry: true, Reason: reason, Decision: DecisionRetry, Mutation: mutation}
}
