// Package errormatch provides types and functions for matching job failure
// signals: exit codes, termination messages, and Kubernetes conditions.
//
// [ExitCodeMatcher] supports In/NotIn set membership against container exit
// codes. Exit code 0 never matches. [RegexMatcher] holds a pattern string
// that callers compile at construction time and pass to [MatchPattern].
//
// Pod-level condition constants ([ConditionOOMKilled], [ConditionEvicted],
// [ConditionDeadlineExceeded], [ConditionAppError]) and the [KnownConditions]
// map are provided for config validation. Non-pod conditions
// ([ConditionPreempted], [ConditionLeaseReturned]) are also defined here for
// use by the retry engine but are not included in [KnownConditions].
package errormatch
