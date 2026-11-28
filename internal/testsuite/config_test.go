package testsuite

import (
	"context"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/pkg/api"
)

func testSpecFromYaml(t *testing.T, name string, yaml string) *api.TestSpec {
	t.Helper()
	testSpec := &api.TestSpec{}
	require.NoError(t, UnmarshalTestCase([]byte(yaml), testSpec))
	testSpec.Name = name
	return testSpec
}

const runScopedConfig = `
config:
  executor:
    kubernetes:
      runScopedPodNames: true
`

func TestGroupByConfig(t *testing.T) {
	tests := map[string]struct {
		specs      map[string]string
		wantGroups [][]string
		wantErr    string
	}{
		"test cases without overrides form one group": {
			specs:      map[string]string{"a": "queue: q", "b": "queue: q"},
			wantGroups: [][]string{{"a", "b"}},
		},
		"the same overrides in another key order share a group": {
			specs: map[string]string{
				"a": "config:\n  executor:\n    a: 1\n    b: 2\n",
				"b": "config:\n  executor:\n    b: 2\n    a: 1\n",
			},
			wantGroups: [][]string{{"a", "b"}},
		},
		"the group without overrides comes first": {
			specs:      map[string]string{"with": runScopedConfig, "without": "queue: q"},
			wantGroups: [][]string{{"without"}, {"with"}},
		},
		"different overrides form groups in the order of their keys": {
			specs: map[string]string{
				"server":   "config:\n  server:\n    a: 1\n",
				"executor": runScopedConfig,
			},
			wantGroups: [][]string{{"executor"}, {"server"}},
		},
		"a component name that is not a file name fails": {
			specs:   map[string]string{"a": "config:\n  ../executor:\n    a: 1\n"},
			wantErr: `component name "../executor"`,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			names := slices.Sorted(maps.Keys(tc.specs))
			specs := make([]*api.TestSpec, len(names))
			for i, specName := range names {
				specs[i] = testSpecFromYaml(t, specName, tc.specs[specName])
			}

			groups, err := groupByConfig(specs)

			if tc.wantErr != "" {
				assert.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			gotGroups := make([][]string, len(groups))
			for i, group := range groups {
				for _, index := range group.indexes {
					gotGroups[i] = append(gotGroups[i], specs[index].Name)
				}
			}
			assert.Equal(t, tc.wantGroups, gotGroups)
		})
	}
}

func TestApplyConfig_WritesOneFilePerComponent(t *testing.T) {
	spec := testSpecFromYaml(t, "a", runScopedConfig+"  server:\n    submission:\n      objectNamePrefix: team\n")
	files := map[string]string{}
	hook := func(_ context.Context, dir string) error {
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		for _, entry := range entries {
			data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
			require.NoError(t, err)
			files[entry.Name()] = string(data)
		}
		return nil
	}

	require.NoError(t, applyConfig(context.Background(), hook, spec.Config))

	require.Len(t, files, 2)
	assert.JSONEq(t, `{"kubernetes": {"runScopedPodNames": true}}`, files["executor.yaml"])
	assert.JSONEq(t, `{"submission": {"objectNamePrefix": "team"}}`, files["server.yaml"])
}
