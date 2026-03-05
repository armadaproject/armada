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

// Condition constants derived from KubernetesReason (pod-level conditions).
const (
	ConditionOOMKilled        = "OOMKilled"
	ConditionEvicted          = "Evicted"
	ConditionDeadlineExceeded = "DeadlineExceeded"
	ConditionAppError         = "AppError"
)

// Condition constants for non-pod error types (used by the retry engine).
const (
	ConditionPreempted     = "Preempted"
	ConditionLeaseReturned = "LeaseReturned"
)

// KnownConditions is the set of pod-level condition strings accepted by the
// executor error categorizer. Non-pod conditions (Preempted, LeaseReturned)
// are excluded because they are not observable from Kubernetes pod status.
var KnownConditions = map[string]bool{
	ConditionOOMKilled:        true,
	ConditionEvicted:          true,
	ConditionDeadlineExceeded: true,
	ConditionAppError:         true,
}
