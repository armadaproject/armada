package leaderelection

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
)

func TestParseMode(t *testing.T) {
	tests := map[string]struct {
		input         string
		expectedMode  Mode
		expectedError bool
	}{
		"valid standalone": {
			input:        "standalone",
			expectedMode: ModeStandalone,
		},
		"valid kubernetes": {
			input:        "kubernetes",
			expectedMode: ModeKubernetes,
		},
		"case insensitive standalone": {
			input:        "Standalone",
			expectedMode: ModeStandalone,
		},
		"case insensitive kubernetes": {
			input:        "KUBERNETES",
			expectedMode: ModeKubernetes,
		},
		"invalid mode": {
			input:         "invalidmode",
			expectedError: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			mode, err := ParseMode(tc.input)
			if tc.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expectedMode, mode)
			}
		})
	}
}

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

func TestCreateLeaderController_EventingesterStandaloneParity(t *testing.T) {
	config := Config{
		Mode:               ModeStandalone,
		LeaseLockName:      "test-lock",
		LeaseLockNamespace: "test-namespace",
		LeaseDuration:      15 * time.Second,
		RenewDeadline:      10 * time.Second,
		RetryPeriod:        2 * time.Second,
		PodName:            "test-pod-eventingester-parity",
	}

	options := &MetricsOptions{MetricsPrefix: "test_eventingester_parity_", MarkLeadingInStandaloneMode: false}

	ctx := armadacontext.Background()
	controller, err := CreateLeaderController(ctx, config, options)

	require.NoError(t, err)
	require.NotNil(t, controller)

	token := controller.GetToken()
	assert.True(t, controller.ValidateToken(token))
}

func TestCreateLeaderController_SchedulerStandaloneParity(t *testing.T) {
	config := Config{
		Mode:               ModeStandalone,
		LeaseLockName:      "test-lock",
		LeaseLockNamespace: "test-namespace",
		LeaseDuration:      15 * time.Second,
		RenewDeadline:      10 * time.Second,
		RetryPeriod:        2 * time.Second,
		PodName:            "test-pod-scheduler-parity",
	}

	options := &MetricsOptions{MetricsPrefix: "test_scheduler_parity_", MarkLeadingInStandaloneMode: true}

	ctx := armadacontext.Background()
	controller, err := CreateLeaderController(ctx, config, options)

	require.NoError(t, err)
	require.NotNil(t, controller)

	token := controller.GetToken()
	assert.True(t, controller.ValidateToken(token))
}

// TestCreateLeaderController_StandaloneModeIsCaseInsensitive verifies mode matching is case-insensitive
func TestCreateLeaderController_StandaloneModeIsCaseInsensitive(t *testing.T) {
	modes := []string{"standalone", "Standalone", "STANDALONE", "StandAlone"}

	for _, mode := range modes {
		t.Run(mode, func(t *testing.T) {
			parsedMode, err := ParseMode(mode)
			require.NoError(t, err)

			config := Config{
				Mode:               parsedMode,
				LeaseLockName:      "test-lock",
				LeaseLockNamespace: "test-namespace",
				LeaseDuration:      15 * time.Second,
				RenewDeadline:      10 * time.Second,
				RetryPeriod:        2 * time.Second,
				PodName:            "test-pod-case-" + mode,
			}

			options := &MetricsOptions{MetricsPrefix: "test_standalone_case_" + mode + "_", MarkLeadingInStandaloneMode: false}

			ctx := armadacontext.Background()
			controller, err := CreateLeaderController(ctx, config, options)

			require.NoError(t, err)
			require.NotNil(t, controller)
		})
	}
}

// TestCreateLeaderController_KubernetesMode tests kubernetes mode creation.
// Skips if no cluster config available (expected in unit test environments).
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

// TestCreateLeaderController_KubernetesModeIsCaseInsensitive verifies mode matching is case-insensitive
func TestCreateLeaderController_KubernetesModeIsCaseInsensitive(t *testing.T) {
	modes := []string{"kubernetes", "Kubernetes", "KUBERNETES", "KuberNetes"}

	for _, mode := range modes {
		t.Run(mode, func(t *testing.T) {
			parsedMode, err := ParseMode(mode)
			require.NoError(t, err)

			config := Config{
				Mode:               parsedMode,
				LeaseLockName:      "test-lock",
				LeaseLockNamespace: "test-namespace",
				LeaseDuration:      15 * time.Second,
				RenewDeadline:      10 * time.Second,
				RetryPeriod:        2 * time.Second,
				PodName:            "test-pod-k8s-case-" + mode,
			}

			options := &MetricsOptions{MetricsPrefix: "test_kubernetes_case_" + mode + "_", MarkLeadingInStandaloneMode: false}

			ctx := armadacontext.Background()
			controller, err := CreateLeaderController(ctx, config, options)

			if err != nil {
				assert.NotContains(t, err.Error(), "not a valid leader mode")
			} else {
				require.NotNil(t, controller)
			}
		})
	}
}

// TestCreateLeaderController_OptionsIndependence verifies that Options don't affect controller type creation
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

func TestToSchedulerLeaderConfig_PreservesValidTimingValues(t *testing.T) {
	config := Config{
		Mode:               ModeKubernetes,
		LeaseLockName:      "test-lock",
		LeaseLockNamespace: "test-namespace",
		LeaseDuration:      15 * time.Second,
		RenewDeadline:      10 * time.Second,
		RetryPeriod:        2 * time.Second,
		PodName:            "test-pod-valid-timing",
	}

	schedulerConfig := toSchedulerLeaderConfig(config)

	assert.Equal(t, config.Mode.String(), schedulerConfig.Mode)
	assert.Equal(t, config.LeaseLockName, schedulerConfig.LeaseLockName)
	assert.Equal(t, config.LeaseLockNamespace, schedulerConfig.LeaseLockNamespace)
	assert.Equal(t, config.LeaseDuration, schedulerConfig.LeaseDuration)
	assert.Equal(t, config.RenewDeadline, schedulerConfig.RenewDeadline)
	assert.Equal(t, config.RetryPeriod, schedulerConfig.RetryPeriod)
	assert.Equal(t, config.PodName, schedulerConfig.PodName)

	assert.Greater(t, schedulerConfig.LeaseDuration, schedulerConfig.RenewDeadline)
	assert.Greater(t, schedulerConfig.RenewDeadline, time.Duration(float64(schedulerConfig.RetryPeriod)*1.2))
}

func TestToSchedulerLeaderConfig_PreservesInvalidTimingRelationships(t *testing.T) {
	tests := map[string]struct {
		leaseDuration time.Duration
		renewDeadline time.Duration
		retryPeriod   time.Duration
	}{
		"lease duration equal renew deadline": {
			leaseDuration: 10 * time.Second,
			renewDeadline: 10 * time.Second,
			retryPeriod:   2 * time.Second,
		},
		"lease duration below renew deadline": {
			leaseDuration: 9 * time.Second,
			renewDeadline: 10 * time.Second,
			retryPeriod:   2 * time.Second,
		},
		"renew deadline equal retry period times jitter factor": {
			leaseDuration: 20 * time.Second,
			renewDeadline: 12 * time.Second,
			retryPeriod:   10 * time.Second,
		},
		"renew deadline below retry period times jitter factor": {
			leaseDuration: 20 * time.Second,
			renewDeadline: 11 * time.Second,
			retryPeriod:   10 * time.Second,
		},
	}

	for name, tc := range tests {
			t.Run(name, func(t *testing.T) {
			config := Config{
				Mode:               ModeKubernetes,
				LeaseLockName:      "test-lock",
				LeaseLockNamespace: "test-namespace",
				LeaseDuration:      tc.leaseDuration,
				RenewDeadline:      tc.renewDeadline,
				RetryPeriod:        tc.retryPeriod,
				PodName:            "test-pod-invalid-timing",
			}

			schedulerConfig := toSchedulerLeaderConfig(config)

			assert.Equal(t, config.LeaseDuration, schedulerConfig.LeaseDuration)
			assert.Equal(t, config.RenewDeadline, schedulerConfig.RenewDeadline)
			assert.Equal(t, config.RetryPeriod, schedulerConfig.RetryPeriod)

			if tc.leaseDuration <= tc.renewDeadline {
				assert.LessOrEqual(t, schedulerConfig.LeaseDuration, schedulerConfig.RenewDeadline)
			}

			if float64(tc.renewDeadline) <= float64(tc.retryPeriod)*1.2 {
				assert.LessOrEqual(t, float64(schedulerConfig.RenewDeadline), float64(schedulerConfig.RetryPeriod)*1.2)
			}
		})
	}
}
