package retry

import (
	"fmt"

	"github.com/armadaproject/armada/pkg/armadaevents"
)

const (
	reasonMatchFail  = "matched rule: Fail"
	reasonMatchRetry = "matched rule: Retry"
	reasonDefault    = "no rule matched, using default action"
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

// Evaluate applies the policy rules to runError and returns a retry decision.
//
// failureCount and totalRuns include the run being evaluated, so the first
// call for a job has failureCount=1 and zero retries consumed. RetryLimit and
// globalMaxRetries both count retries (not failures): RetryLimit=3 allows 3
// retries after the initial failure, i.e. 4 attempts total. A limit of 0
// means unlimited (the global cap still applies to policy limits).
//
// policy must not be nil; runError may be nil (treated as "no decision").
func (e *Engine) Evaluate(policy *Policy, runError *armadaevents.Error, failureCount uint32, totalRuns uint) Result {
	if runError == nil {
		return Result{ShouldRetry: false, Reason: "no error information available"}
	}

	retriesUsed := uint(0)
	if totalRuns > 0 {
		retriesUsed = totalRuns - 1
	}
	if e.globalMaxRetries > 0 && retriesUsed >= e.globalMaxRetries {
		return Result{
			ShouldRetry: false,
			Reason:      fmt.Sprintf("global max retries exceeded (%d/%d)", retriesUsed, e.globalMaxRetries),
		}
	}

	matched := matchRules(policy.Rules, matchInput{
		condition:          extractCondition(runError),
		exitCode:           extractExitCode(runError),
		terminationMessage: extractTerminationMessage(runError),
		isContainerFailure: runError.GetPodError() != nil,
		category:           extractCategory(runError),
		subcategory:        extractSubcategory(runError),
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
		return Result{ShouldRetry: false, Reason: reason}
	}

	policyRetriesUsed := uint32(0)
	if failureCount > 0 {
		policyRetriesUsed = failureCount - 1
	}
	if policy.RetryLimit > 0 && policyRetriesUsed >= policy.RetryLimit {
		return Result{
			ShouldRetry: false,
			Reason:      fmt.Sprintf("policy retry limit exceeded (%d/%d)", policyRetriesUsed, policy.RetryLimit),
		}
	}

	return Result{ShouldRetry: true, Reason: reason}
}
