package errormatch

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMatchExitCode(t *testing.T) {
	tests := map[string]struct {
		matcher  *ExitCodeMatcher
		exitCode int32
		expected bool
	}{
		"In matches": {
			matcher:  &ExitCodeMatcher{Operator: ExitCodeOperatorIn, Values: []int32{74, 75}},
			exitCode: 74,
			expected: true,
		},
		"In does not match": {
			matcher:  &ExitCodeMatcher{Operator: ExitCodeOperatorIn, Values: []int32{74, 75}},
			exitCode: 1,
			expected: false,
		},
		"NotIn matches when code absent": {
			matcher:  &ExitCodeMatcher{Operator: ExitCodeOperatorNotIn, Values: []int32{1, 2}},
			exitCode: 42,
			expected: true,
		},
		"NotIn does not match when code present": {
			matcher:  &ExitCodeMatcher{Operator: ExitCodeOperatorNotIn, Values: []int32{1, 2}},
			exitCode: 1,
			expected: false,
		},
		"exit code 0 never matches In": {
			matcher:  &ExitCodeMatcher{Operator: ExitCodeOperatorIn, Values: []int32{0}},
			exitCode: 0,
			expected: false,
		},
		"exit code 0 never matches NotIn": {
			matcher:  &ExitCodeMatcher{Operator: ExitCodeOperatorNotIn, Values: []int32{1}},
			exitCode: 0,
			expected: false,
		},
		"nil matcher returns false": {
			matcher:  nil,
			exitCode: 1,
			expected: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, MatchExitCode(tc.matcher, tc.exitCode))
		})
	}
}

func TestMatchPattern(t *testing.T) {
	tests := map[string]struct {
		pattern  string
		value    string
		expected bool
	}{
		"matches": {
			pattern:  "(?i)cuda.*error",
			value:    "CUDA memory error on device 0",
			expected: true,
		},
		"does not match": {
			pattern:  "(?i)cuda.*error",
			value:    "segfault",
			expected: false,
		},
		"empty value never matches": {
			pattern:  ".*",
			value:    "",
			expected: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			re := regexp.MustCompile(tc.pattern)
			assert.Equal(t, tc.expected, MatchPattern(re, tc.value))
		})
	}
}
