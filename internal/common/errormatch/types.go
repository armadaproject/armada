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

const (
	ConditionOOMKilled        = "OOMKilled"
	ConditionEvicted          = "Evicted"
	ConditionDeadlineExceeded = "DeadlineExceeded"
)

// KnownConditions is the set of valid condition strings for config validation.
var KnownConditions = map[string]bool{
	ConditionOOMKilled:        true,
	ConditionEvicted:          true,
	ConditionDeadlineExceeded: true,
}
