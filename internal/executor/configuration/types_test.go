package configuration

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecognizedTaintsLiteralAndRegexMatching(t *testing.T) {
	for _, scope := range []string{"user", "admin"} {
		t.Run(scope, func(t *testing.T) {
			for _, tc := range []struct {
				name    string
				pattern string
				matches map[string]bool
			}{
				{
					name: "literal dotted key", pattern: "nmc2.ai/cpe-test",
					matches: map[string]bool{
						"nmc2.ai/cpe-test":        true,
						"nmc2.ai/cpe-testx":       false,
						"prefix-nmc2.ai/cpe-test": false,
						"nmc2Xai/cpe-test":        false,
					},
				},
				{
					name: "explicit suffix regex", pattern: `nmc2\.ai/cpe-test.*`,
					matches: map[string]bool{
						"nmc2.ai/cpe-test":        true,
						"nmc2.ai/cpe-testx":       true,
						"prefix-nmc2.ai/cpe-test": false,
						"nmc2Xai/cpe-test":        false,
					},
				},
				{
					name: "numeric suffix regex", pattern: `nmc2\.ai/cpe-test[0-9]+`,
					matches: map[string]bool{
						"nmc2.ai/cpe-test12":  true,
						"nmc2.ai/cpe-test":    false,
						"nmc2.ai/cpe-test12x": false,
					},
				},
				{
					name: "alternation matches complete keys", pattern: "foo|bar",
					matches: map[string]bool{
						"foo":   true,
						"bar":   true,
						"foox":  false,
						"xbar":  false,
						"xfoox": false,
					},
				},
				{
					name: "anchors explicitly select regex semantics", pattern: "^nmc2.ai/cpe-test$",
					matches: map[string]bool{
						"nmc2.ai/cpe-test":  true,
						"nmc2Xai/cpe-test":  true,
						"nmc2.ai/cpe-testx": false,
					},
				},
				{
					name: "escaped dot selects regex semantics", pattern: `nmc2\.ai/cpe-test`,
					matches: map[string]bool{
						"nmc2.ai/cpe-test":  true,
						"nmc2Xai/cpe-test":  false,
						"nmc2.ai/cpe-testx": false,
					},
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					config := KubernetesConfiguration{}
					category := UserRecognizedTaintCategory
					if scope == "user" {
						config.RecognizedTaints.Keys = []string{tc.pattern}
					} else {
						category = "platform"
						config.RecognizedTaints.Admin = []RecognizedTaintAdminConfiguration{{Category: category, Keys: []string{tc.pattern}}}
					}
					matcher, err := config.NewRecognizedTaints()
					require.NoError(t, err)
					for key, expectedMatch := range tc.matches {
						taint, ok := matcher.MatchRecognizedTaint(key)
						assert.Equal(t, expectedMatch, ok, "unexpected match result for %q", key)
						if expectedMatch {
							assert.Equal(t, category, taint.Category)
							assert.Equal(t, scope == "user", taint.UserFacing)
						}
					}
				})
			}
		})
	}
}
