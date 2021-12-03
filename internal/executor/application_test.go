package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/internal/executor/configuration"
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

	config.Application.SubmitThreadCount = 1
	assert.NoError(t, validateConfig(config))
	config.Application.SubmitThreadCount = 5
	assert.NoError(t, validateConfig(config))

	config.Application.SubmitThreadCount = 0
	assert.Error(t, validateConfig(config))
	config.Application.SubmitThreadCount = -5
	assert.Error(t, validateConfig(config))
}

func Test_ValidateConfig_When_UpdateThreadCount(t *testing.T) {
	config := createBasicValidExecutorConfiguration()

	config.Application.UpdateThreadCount = 1
	assert.NoError(t, validateConfig(config))
	config.Application.UpdateThreadCount = 5
	assert.NoError(t, validateConfig(config))

	config.Application.UpdateThreadCount = 0
	assert.Error(t, validateConfig(config))
	config.Application.UpdateThreadCount = -5
	assert.Error(t, validateConfig(config))
}

func Test_ValidateConfig_When_DeleteThreadCount_LessThanOrEqualToZero(t *testing.T) {
	config := createBasicValidExecutorConfiguration()

	config.Application.DeleteThreadCount = 1
	assert.NoError(t, validateConfig(config))
	config.Application.DeleteThreadCount = 5
	assert.NoError(t, validateConfig(config))

	config.Application.DeleteThreadCount = 0
	assert.Error(t, validateConfig(config))
	config.Application.DeleteThreadCount = -5
	assert.Error(t, validateConfig(config))
}

func createBasicValidExecutorConfiguration() configuration.ExecutorConfiguration {
	return configuration.ExecutorConfiguration{
		Application: configuration.ApplicationConfiguration{
			SubmitThreadCount: 1,
			UpdateThreadCount: 1,
			DeleteThreadCount: 1,
		},
	}
}
