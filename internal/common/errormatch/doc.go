// Package errormatch provides types and functions for matching job failure
// signals: exit codes, termination messages, and Kubernetes conditions.
//
// [ExitCodeMatcher] supports In/NotIn set membership against container exit
// codes. Exit code 0 never matches. [RegexMatcher] holds a pattern string
// that callers compile at construction time and pass to [MatchPattern].
//
// The condition constants ([ConditionOOMKilled], [ConditionEvicted],
// [ConditionDeadlineExceeded]) and [KnownConditions] map are provided for
// config validation.
package errormatch
