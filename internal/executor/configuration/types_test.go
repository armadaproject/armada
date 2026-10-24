package configuration

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueueableTaintsLiteralAndRegexMatching(t *testing.T) {
	for _, scope := range []string{"user", "admin"} {
		t.Run(scope, func(t *testing.T) {
			for _, tc := range []struct {
				name    string
				pattern string
				matches []string
				rejects []string
			}{
				{
					name: "literal dotted key", pattern: "nmc2.ai/cpe-test",
					matches: []string{"nmc2.ai/cpe-test"},
					rejects: []string{"nmc2.ai/cpe-testx", "prefix-nmc2.ai/cpe-test", "nmc2Xai/cpe-test"},
				},
				{
					name: "explicit suffix regex", pattern: `nmc2\.ai/cpe-test.*`,
					matches: []string{"nmc2.ai/cpe-test", "nmc2.ai/cpe-testx"},
					rejects: []string{"prefix-nmc2.ai/cpe-test", "nmc2Xai/cpe-test"},
				},
				{
					name: "numeric suffix regex", pattern: `nmc2\.ai/cpe-test[0-9]+`,
					matches: []string{"nmc2.ai/cpe-test12"},
					rejects: []string{"nmc2.ai/cpe-test", "nmc2.ai/cpe-test12x"},
				},
				{
					name: "alternation matches complete keys", pattern: "foo|bar",
					matches: []string{"foo", "bar"},
					rejects: []string{"foox", "xbar", "xfoox"},
				},
				{
					name: "anchors explicitly select regex semantics", pattern: "^nmc2.ai/cpe-test$",
					matches: []string{"nmc2.ai/cpe-test", "nmc2Xai/cpe-test"},
					rejects: []string{"nmc2.ai/cpe-testx"},
				},
				{
					name: "escaped dot selects regex semantics", pattern: `nmc2\.ai/cpe-test`,
					matches: []string{"nmc2.ai/cpe-test"},
					rejects: []string{"nmc2Xai/cpe-test", "nmc2.ai/cpe-testx"},
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					config := KubernetesConfiguration{}
					category := UserQueueableTaintCategory
					if scope == "user" {
						config.QueueableTaints.Keys = []string{tc.pattern}
					} else {
						category = "platform"
						config.QueueableTaints.Admin = []QueueableTaintAdminConfiguration{{Category: category, Keys: []string{tc.pattern}}}
					}
					matcher, err := config.NewQueueableTaints()
					require.NoError(t, err)
					for _, key := range tc.matches {
						taint, ok := matcher.MatchQueueableTaint(key)
						assert.True(t, ok, "expected %q to match", key)
						assert.Equal(t, category, taint.Category)
						assert.Equal(t, scope == "user", taint.UserFacing)
					}
					for _, key := range tc.rejects {
						_, ok := matcher.MatchQueueableTaint(key)
						assert.False(t, ok, "expected %q not to match", key)
					}
				})
			}
		})
	}
}
