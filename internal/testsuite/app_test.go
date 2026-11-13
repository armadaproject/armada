package testsuite

import (
	"context"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/pkg/api"
)

func TestUnmarshalTestCase_ConfigNextToPodSpecs(t *testing.T) {
	yaml := `
queue: e2e-test-queue
jobs:
  - namespace: default
    podSpec:
      containers:
        - name: fail
          image: alpine:3.20.0
config:
  executor:
    application:
      errorCategories:
        defaultCategory: config_override
---
timeout: "300s"
expectedEvents:
  - submitted:
`
	testSpec := &api.TestSpec{}

	require.NoError(t, UnmarshalTestCase([]byte(yaml), testSpec))

	config, err := canonicalConfig(testSpec.Config)
	require.NoError(t, err)
	assert.Equal(t, `{"executor":{"application":{"errorCategories":{"defaultCategory":"config_override"}}}}`, config)
	assert.Len(t, testSpec.ExpectedEvents, 1)
}

func TestRunTests_ConfigOverrides(t *testing.T) {
	type hookCall struct {
		withOverrides bool
		cancelled     bool
	}
	tests := map[string]struct {
		withHook bool
		// applyErr is the error of the hook call that applies the overrides.
		applyErr error
		// cancelOnApply cancels the run while the hook applies the overrides, as a signal does.
		cancelOnApply bool
		wantSkip      string
		wantFailure   string
		wantCalls     []hookCall
	}{
		"a test case with overrides is skipped without a hook": {
			wantSkip: "the test case has config overrides, and no config hook is set",
		},
		"the hook removes leftover overrides first, and a failed apply fails the test case": {
			withHook:    true,
			applyErr:    assert.AnError,
			wantFailure: "failed to apply config overrides: " + assert.AnError.Error(),
			wantCalls:   []hookCall{{withOverrides: false}, {withOverrides: true}, {withOverrides: false}},
		},
		"a cancelled run still removes the overrides": {
			withHook:      true,
			applyErr:      assert.AnError,
			cancelOnApply: true,
			wantFailure:   "failed to apply config overrides: " + assert.AnError.Error(),
			wantCalls:     []hookCall{{withOverrides: false}, {withOverrides: true, cancelled: true}, {withOverrides: false}},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			app := New()
			app.Out = io.Discard
			var calls []hookCall
			if tc.withHook {
				app.Params.ConfigHook = func(hookCtx context.Context, dir string) error {
					entries, err := os.ReadDir(dir)
					require.NoError(t, err)
					withOverrides := len(entries) > 0
					if withOverrides && tc.cancelOnApply {
						cancel()
					}
					calls = append(calls, hookCall{withOverrides: withOverrides, cancelled: hookCtx.Err() != nil})
					if withOverrides {
						return tc.applyErr
					}
					return nil
				}
			}

			report, err := app.RunTests(ctx, []*api.TestSpec{testSpecFromYaml(t, "a", runScopedConfig)})

			require.NoError(t, err)
			require.Len(t, report.TestCaseReports, 1)
			assert.Equal(t, tc.wantSkip, report.TestCaseReports[0].SkipReason)
			if tc.wantSkip != "" {
				assert.Equal(t, 1, report.NumSkipped())
			}
			assert.Equal(t, tc.wantFailure, report.TestCaseReports[0].FailureReason)
			assert.Equal(t, tc.wantCalls, calls)
		})
	}
}
