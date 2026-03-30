package retry

import (
	"fmt"

	"github.com/armadaproject/armada/pkg/armadaevents"
)

// Pre-allocated reason strings to avoid per-call allocations in the hot path.
var reasonForAction = map[Action]string{
	ActionFail:  "matched rule: Fail",
	ActionRetry: "matched rule: Retry",
}

const reasonDefault = "no rule matched, using default action"

// Engine evaluates retry policies against job run errors to decide whether
// a job should be retried or permanently failed.
type Engine struct {
	globalMaxRetries uint
}

// NewEngine creates a retry engine with the given hard upper limit on retries.
func NewEngine(globalMaxRetries uint) *Engine {
	return &Engine{globalMaxRetries: globalMaxRetries}
}

// Evaluate applies the policy rules to the given error and returns a retry decision.
//
// Parameters:
//   - policy: the retry policy to evaluate (must not be nil)
//   - runError: the error from the failed run (may be nil)
//   - failureCount: how many times this job has already failed under this policy
//   - totalRuns: the total number of runs for this job across all policies
func (e *Engine) Evaluate(policy *Policy, runError *armadaevents.Error, failureCount uint32, totalRuns uint) Result {
	if runError == nil {
		return Result{ShouldRetry: false, Reason: "no error information available"}
	}

	if e.globalMaxRetries > 0 && totalRuns >= e.globalMaxRetries {
		return Result{
			ShouldRetry: false,
			Reason:      fmt.Sprintf("global max retries exceeded (%d/%d)", totalRuns, e.globalMaxRetries),
		}
	}

	condition := extractCondition(runError)
	exitCode := extractExitCode(runError)
	terminationMessage := extractTerminationMessage(runError)
	categories := extractCategories(runError)

	matched := matchRules(policy.Rules, condition, exitCode, terminationMessage, categories)

	action := policy.DefaultAction
	matchDesc := reasonDefault
	if matched != nil {
		action = matched.Action
		matchDesc = reasonForAction[matched.Action]
	}

	if action == ActionFail {
		return Result{ShouldRetry: false, Reason: matchDesc}
	}

	// Action is Retry. Check the policy-level retry limit.
	// RetryLimit 0 means unlimited (subject to global cap).
	if policy.RetryLimit > 0 && failureCount >= policy.RetryLimit {
		return Result{
			ShouldRetry: false,
			Reason:      fmt.Sprintf("policy retry limit exceeded (%d/%d)", failureCount, policy.RetryLimit),
		}
	}

	return Result{ShouldRetry: true, Reason: matchDesc}
}
