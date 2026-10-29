package configuration

import (
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonconfig "github.com/armadaproject/armada/internal/common/config"
)

func TestWellKnownTaintsConfigDecoding(t *testing.T) {
	v := viper.NewWithOptions(viper.KeyDelimiter("::"))
	v.SetConfigType("yaml")
	err := v.ReadConfig(strings.NewReader(`
kubernetes:
  wellKnownTaints:
    keys:
      - example.com/user
    admin:
      - category: platform
        keys:
          - example.com/maintenance
  toleratedTaints:
    - example.com/legacy
`))
	require.NoError(t, err)
	var config ExecutorConfiguration
	require.NoError(t, v.Unmarshal(&config, commonconfig.CustomHooks...))
	matcher, err := config.Kubernetes.NewWellKnownTaints()
	require.NoError(t, err)

	for _, tc := range []struct {
		key        string
		category   string
		userFacing bool
	}{
		{key: "example.com/user", category: "user", userFacing: true},
		{key: "example.com/maintenance", category: "platform"},
		{key: "example.com/legacy", userFacing: true},
	} {
		t.Run(tc.key, func(t *testing.T) {
			taint, ok := matcher.MatchWellKnownTaint(tc.key)
			require.True(t, ok)
			assert.Equal(t, tc.category, taint.Category)
			assert.Equal(t, tc.userFacing, taint.UserFacing)
		})
	}
}

func TestWellKnownTaintsLiteralAndRegexMatching(t *testing.T) {
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
					category := UserWellKnownTaintCategory
					if scope == "user" {
						config.WellKnownTaints.Keys = []string{tc.pattern}
					} else {
						category = "platform"
						config.WellKnownTaints.Admin = []WellKnownTaintAdminConfiguration{{Category: category, Keys: []string{tc.pattern}}}
					}
					matcher, err := config.NewWellKnownTaints()
					require.NoError(t, err)
					for key, expectedMatch := range tc.matches {
						taint, ok := matcher.MatchWellKnownTaint(key)
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
