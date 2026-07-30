package leaderelection

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
)

func TestCreateLeaderController_StandaloneMode(t *testing.T) {
	tests := map[string]struct {
		provideMetricsOptions bool
		markLeading           bool
		description           string
	}{
		"no metrics options": {
			provideMetricsOptions: false,
			markLeading:           false,
			description:           "No metrics options provided",
		},
		"metrics options without marking leading": {
			provideMetricsOptions: true,
			markLeading:           false,
			description:           "Provide metrics options but do not mark as leading",
		},
		"metrics options with marking leading": {
			provideMetricsOptions: true,
			markLeading:           true,
			description:           "Provide metrics options and mark as leading",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			podName := "test-pod-" + strings.ReplaceAll(name, " ", "-")
			metricsPrefix := "test_" + strings.ReplaceAll(name, " ", "_") + "_"

			config := Config{
				Mode:               ModeStandalone,
				LeaseLockName:      "test-lock",
				LeaseLockNamespace: "test-namespace",
				LeaseDuration:      15 * time.Second,
				RenewDeadline:      10 * time.Second,
				RetryPeriod:        2 * time.Second,
				PodName:            podName,
			}

			var options *MetricsOptions
			if tc.provideMetricsOptions {
				options = &MetricsOptions{
					MetricsPrefix:               metricsPrefix,
					MarkLeadingInStandaloneMode: tc.markLeading,
				}
			}

			ctx := armadacontext.Background()
			controller, err := CreateLeaderController(ctx, config, options)

			require.NoError(t, err, "Failed to create controller: %s", tc.description)
			require.NotNil(t, controller)

			token := controller.GetToken()
			assert.True(t, controller.ValidateToken(token), "Standalone controller should always validate token")
		})
	}
}

func TestCreateLeaderController_KubernetesMode(t *testing.T) {
	config := Config{
		Mode:               ModeKubernetes,
		LeaseLockName:      "test-lock",
		LeaseLockNamespace: "test-namespace",
		LeaseDuration:      15 * time.Second,
		RenewDeadline:      10 * time.Second,
		RetryPeriod:        2 * time.Second,
		PodName:            "test-pod-k8s",
	}

	options := &MetricsOptions{MetricsPrefix: "test_k8s_mode_", MarkLeadingInStandaloneMode: false}

	ctx := armadacontext.Background()
	controller, err := CreateLeaderController(ctx, config, options)

	if err != nil {
		assert.NotContains(t, err.Error(), "not a valid leader mode")
		assert.Contains(t, err.Error(), "error creating kubernetes client")
		t.Skipf("Skipping kubernetes mode test - no cluster config available: %v", err)
	} else {
		require.NotNil(t, controller, "Controller should be created if cluster config is available")
	}
}

func TestCreateLeaderController_OptionsIndependence(t *testing.T) {
	config := Config{
		Mode:               ModeStandalone,
		LeaseLockName:      "test-lock",
		LeaseLockNamespace: "test-namespace",
		LeaseDuration:      15 * time.Second,
		RenewDeadline:      10 * time.Second,
		RetryPeriod:        2 * time.Second,
		PodName:            "test-pod-options-independence",
	}

	optionsAllTrue := &MetricsOptions{MetricsPrefix: "test_options_all_true_", MarkLeadingInStandaloneMode: true}

	ctx := armadacontext.Background()
	controller1, err1 := CreateLeaderController(ctx, config, optionsAllTrue)
	require.NoError(t, err1)
	require.NotNil(t, controller1)

	config.PodName = "test-pod-options-independence-false"
	optionsAllFalse := &MetricsOptions{MetricsPrefix: "test_options_all_false_", MarkLeadingInStandaloneMode: false}

	controller2, err2 := CreateLeaderController(ctx, config, optionsAllFalse)
	require.NoError(t, err2)
	require.NotNil(t, controller2)

	token1 := controller1.GetToken()
	assert.True(t, controller1.ValidateToken(token1))

	token2 := controller2.GetToken()
	assert.True(t, controller2.ValidateToken(token2))
}
