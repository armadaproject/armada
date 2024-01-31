package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"

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

func createBasicValidExecutorConfiguration() configuration.ExecutorConfiguration {
	return configuration.ExecutorConfiguration{
		Application: configuration.ApplicationConfiguration{
			SubmitConcurrencyLimit: 1,
			UpdateConcurrencyLimit: 1,
			DeleteConcurrencyLimit: 1,
		},
	}
}
