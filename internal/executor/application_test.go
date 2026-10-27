package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/constants"
	"github.com/armadaproject/armada/internal/executor/configuration"
)

func Test_ValidateConfig_When_AvoidNodeLabelsOnRetry_MissingFrom_TrackedNodeLabels_Fails(t *testing.T) {
	config := createBasicValidExecutorConfiguration()

	config.Kubernetes.TrackedNodeLabels = []string{"host"}
	config.Kubernetes.AvoidNodeLabelsOnRetry = []string{"host", "cluster"}
	assert.Error(t, validateConfig(config))
}

func Test_ValidateConfig_When_AvoidNodeLabelsOnRetry_And_TrackedNodeLabels_Match_Succeeds(t *testing.T) {
	config := createBasicValidExecutorConfiguration()

	config.Kubernetes.TrackedNodeLabels = []string{"host", "cluster"}
	config.Kubernetes.AvoidNodeLabelsOnRetry = []string{"host", "cluster"}
	assert.NoError(t, validateConfig(config))
}

func Test_ValidateConfig_When_TrackedNodeLabels_MissingFrom_AvoidNodeLabelsOnRetry_Succeeds(t *testing.T) {
	config := createBasicValidExecutorConfiguration()

	config.Kubernetes.TrackedNodeLabels = []string{"host", "cluster"}
	config.Kubernetes.AvoidNodeLabelsOnRetry = []string{"host"}
	assert.NoError(t, validateConfig(config))
}

func Test_ValidateConfig_When_SubmitThreadCount(t *testing.T) {
	config := createBasicValidExecutorConfiguration()

	config.Application.SubmitConcurrencyLimit = 1
	assert.NoError(t, validateConfig(config))
	config.Application.SubmitConcurrencyLimit = 5
	assert.NoError(t, validateConfig(config))

	config.Application.SubmitConcurrencyLimit = 0
	assert.Error(t, validateConfig(config))
	config.Application.SubmitConcurrencyLimit = -5
	assert.Error(t, validateConfig(config))
}

func Test_ValidateConfig_When_UpdateThreadCount(t *testing.T) {
	config := createBasicValidExecutorConfiguration()

	config.Application.UpdateConcurrencyLimit = 1
	assert.NoError(t, validateConfig(config))
	config.Application.UpdateConcurrencyLimit = 5
	assert.NoError(t, validateConfig(config))

	config.Application.UpdateConcurrencyLimit = 0
	assert.Error(t, validateConfig(config))
	config.Application.UpdateConcurrencyLimit = -5
	assert.Error(t, validateConfig(config))
}

func Test_ValidateConfig_When_DeleteThreadCount_LessThanOrEqualToZero(t *testing.T) {
	config := createBasicValidExecutorConfiguration()

	config.Application.DeleteConcurrencyLimit = 1
	assert.NoError(t, validateConfig(config))
	config.Application.DeleteConcurrencyLimit = 5
	assert.NoError(t, validateConfig(config))

	config.Application.DeleteConcurrencyLimit = 0
	assert.Error(t, validateConfig(config))
	config.Application.DeleteConcurrencyLimit = -5
	assert.Error(t, validateConfig(config))
}

func TestValidateConfig_WhenQueueableTaintPatternIsInvalid_Fails(t *testing.T) {
	config := createBasicValidExecutorConfiguration()
	config.Kubernetes.QueueableTaints.Keys = []string{"["}
	assert.Error(t, validateConfig(config))

	config.Kubernetes.QueueableTaints.Keys = nil
	config.Kubernetes.QueueableTaints.Admin = []configuration.QueueableTaintAdminConfiguration{
		{Category: "platform", Keys: []string{"["}},
	}
	assert.Error(t, validateConfig(config))

	config.Kubernetes.QueueableTaints.Admin = []configuration.QueueableTaintAdminConfiguration{
		{Keys: []string{"admin.example.com/maintenance"}},
	}
	assert.Error(t, validateConfig(config))

	config.Kubernetes.QueueableTaints.Admin = []configuration.QueueableTaintAdminConfiguration{
		{Category: configuration.UserQueueableTaintCategory, Keys: []string{"admin.example.com/maintenance"}},
	}
	assert.Error(t, validateConfig(config))
}

func TestKubernetesConfigurationNewQueueableTaints(t *testing.T) {
	kubernetesConfig := configuration.KubernetesConfiguration{
		ToleratedTaints: []string{"legacy.example.com/dedicated"},
		QueueableTaints: configuration.QueueableTaintsConfiguration{
			Keys: []string{"user.example.com/dedicated"},
			Admin: []configuration.QueueableTaintAdminConfiguration{
				{Category: "platform", Keys: []string{"platform.example.com/.*", "legacy\\.example\\.com/.*"}},
			},
		},
	}

	queueableTaints, err := kubernetesConfig.NewQueueableTaints()
	require.NoError(t, err)

	taint, ok := queueableTaints.MatchQueueableTaint("user.example.com/dedicated")
	assert.True(t, ok)
	assert.Equal(t, configuration.UserQueueableTaintCategory, taint.Category)
	assert.True(t, taint.UserFacing)

	taint, ok = queueableTaints.MatchQueueableTaint("legacy.example.com/dedicated")
	assert.True(t, ok)
	assert.Empty(t, taint.Category)
	assert.True(t, taint.UserFacing)

	// A longer key must use the configured admin regex, not the legacy literal.
	taint, ok = queueableTaints.MatchQueueableTaint("legacy.example.com/dedicated-extra")
	assert.True(t, ok)
	assert.Equal(t, "platform", taint.Category)
	assert.False(t, taint.UserFacing)

	taint, ok = queueableTaints.MatchQueueableTaint(constants.ReservationTaintKey)
	assert.True(t, ok)
	assert.Empty(t, taint.Category)
	assert.True(t, taint.UserFacing)

	taint, ok = queueableTaints.MatchQueueableTaint("platform.example.com/gpu")
	assert.True(t, ok)
	assert.Equal(t, "platform", taint.Category)
	assert.False(t, taint.UserFacing)

	_, ok = queueableTaints.MatchQueueableTaint("other.example.com/taint")
	assert.False(t, ok)
}

func createBasicValidExecutorConfiguration() configuration.ExecutorConfiguration {
	return configuration.ExecutorConfiguration{
		Application: configuration.ApplicationConfiguration{
			SubmitConcurrencyLimit: 1,
			UpdateConcurrencyLimit: 1,
			DeleteConcurrencyLimit: 1,
		},
	}
}
