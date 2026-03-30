package errormatch

// ExitCodeOperator is a set membership operator: In or NotIn.
type ExitCodeOperator string

const (
	ExitCodeOperatorIn    ExitCodeOperator = "In"
	ExitCodeOperatorNotIn ExitCodeOperator = "NotIn"
)

// ExitCodeMatcher specifies an operator and a set of exit code values.
type ExitCodeMatcher struct {
	Operator ExitCodeOperator `yaml:"operator"`
	Values   []int32          `yaml:"values"`
}

// RegexMatcher specifies a regex pattern as a string.
type RegexMatcher struct {
	Pattern string `yaml:"pattern"`
}

// Condition constants derived from KubernetesReason.
// OOMKilled is a container-level condition (container.State.Terminated.Reason).
// Evicted and DeadlineExceeded are pod-level conditions (pod.Status.Reason).
const (
	ConditionOOMKilled        = "OOMKilled"
	ConditionEvicted          = "Evicted"
	ConditionDeadlineExceeded = "DeadlineExceeded"
)

// Condition constants for non-pod error types (used by the retry engine).
// These are not observable from Kubernetes pod status, so the executor
// categorizer cannot match them. ConditionAppError is the catch-all for
// a PodError whose KubernetesReason is AppError (i.e. container failed
// without a recognised pod-level reason).
const (
	ConditionPreempted     = "Preempted"
	ConditionLeaseReturned = "LeaseReturned"
	ConditionAppError      = "AppError"
)

// KnownConditions is the set of condition strings accepted by the executor
// error categorizer. Only conditions that map to real Kubernetes reason
// strings are included.
var KnownConditions = map[string]bool{
	ConditionOOMKilled:        true,
	ConditionEvicted:          true,
	ConditionDeadlineExceeded: true,
}
