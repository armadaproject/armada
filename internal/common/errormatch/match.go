package errormatch

import "regexp"

// MatchExitCode returns true if the exit code matches the matcher.
// Exit code 0 never matches (successful containers are not failures).
func MatchExitCode(matcher *ExitCodeMatcher, exitCode int32) bool {
	if exitCode == 0 {
		return false
	}
	switch matcher.Operator {
	case ExitCodeOperatorIn:
		for _, v := range matcher.Values {
			if exitCode == v {
				return true
			}
		}
	case ExitCodeOperatorNotIn:
		for _, v := range matcher.Values {
			if exitCode == v {
				return false
			}
		}
		return true
	}
	return false
}

// MatchPattern returns true if the value matches the compiled regex.
// Empty values never match.
func MatchPattern(re *regexp.Regexp, value string) bool {
	return value != "" && re.MatchString(value)
}
