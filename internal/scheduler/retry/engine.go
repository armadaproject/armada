package retry

import (
	"github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// Engine evaluates retry policies for job failures.
type Engine struct {
	resolver *PolicyResolver
	matcher  *Matcher
	log      *logrus.Entry
}

// NewEngine creates a new retry policy Engine.
// Returns an error if regex patterns in the configuration fail to compile.
func NewEngine(config configuration.RetryPolicyConfig) (*Engine, error) {
	resolver, err := NewPolicyResolver(config)
	if err != nil {
		return nil, err
	}
	return &Engine{
		resolver: resolver,
		matcher:  NewMatcher(),
		log:      logrus.WithField("component", "RetryPolicyEngine"),
	}, nil
}

func (e *Engine) Enabled() bool {
	return e.resolver.Enabled()
}

// GlobalMaxRetries: the value equals retries allowed (e.g., 1 means 1 retry, 2 total runs).
func (e *Engine) GlobalMaxRetries() uint {
	return e.resolver.GlobalMaxRetries()
}

// Result contains the result of evaluating a failure against the retry policy.
type Result struct {
	// Action to take (Fail or Retry).
	Action configuration.Action
	// ShouldRequeue is true if the job should be requeued.
	ShouldRequeue bool
	// IncrementFailureCount is true if the failure count should be incremented.
	IncrementFailureCount bool
}

// Evaluate evaluates the failure against the retry policy and returns the action to take.
func (e *Engine) Evaluate(
	queueRetryPolicy string,
	failureInfo *armadaevents.FailureInfo,
	currentFailureCount uint32,
	totalRuns uint,
) Result {
	if !e.Enabled() {
		return e.fallbackToPodCheck(failureInfo)
	}

	// totalRuns is NumAttempts (including the current failing run).
	// GlobalMaxRetries is the number of retries allowed beyond the initial run.
	// Example: GlobalMaxRetries=5, totalRuns=6 → 6 > 5 → no more retries (5 retries used).
	if totalRuns > e.GlobalMaxRetries() {
		e.log.WithFields(logrus.Fields{
			"totalRuns":        totalRuns,
			"globalMaxRetries": e.GlobalMaxRetries(),
		}).Debug("Global max retries reached")
		return failResult()
	}

	// Get the policy for this queue
	policy := e.resolver.GetPolicy(queueRetryPolicy)
	if policy == nil {
		return e.fallbackToPodCheck(failureInfo)
	}

	// Match failure against policy rules
	action := e.matcher.Match(failureInfo, policy.Rules)

	e.log.WithFields(logrus.Fields{
		"queueRetryPolicy":    queueRetryPolicy,
		"action":              action,
		"condition":           failureInfo.GetCondition(),
		"exitCode":            failureInfo.GetExitCode(),
		"terminationMessage":  failureInfo.GetTerminationMessage(),
		"currentFailureCount": currentFailureCount,
		"policyRetryLimit":    policy.RetryLimit,
	}).Debug("Evaluated retry policy")

	return e.applyAction(action, policy, currentFailureCount)
}

// fallbackToPodCheck is used when retry policy is disabled or no policy found.
func (e *Engine) fallbackToPodCheck(failureInfo *armadaevents.FailureInfo) Result {
	if failureInfo == nil || !failureInfo.PodCheckRetryable {
		return failResult()
	}
	return retryResult()
}

func (e *Engine) applyAction(
	action configuration.Action,
	policy *configuration.Policy,
	currentFailureCount uint32,
) Result {
	if action != configuration.ActionRetry {
		return failResult()
	}

	// currentFailureCount is BEFORE this failure, so retryLimit:3 allows 3 retries (0,1,2 pass; 3 fails)
	if uint(currentFailureCount) >= policy.RetryLimit {
		e.log.WithFields(logrus.Fields{
			"currentFailureCount": currentFailureCount,
			"retryLimit":          policy.RetryLimit,
		}).Debug("Retry limit exceeded")
		return failResult()
	}

	return retryResult()
}

func retryResult() Result {
	return Result{
		Action:                configuration.ActionRetry,
		ShouldRequeue:         true,
		IncrementFailureCount: true,
	}
}

func failResult() Result {
	return Result{
		Action:                configuration.ActionFail,
		ShouldRequeue:         false,
		IncrementFailureCount: false,
	}
}
