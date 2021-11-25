package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/internal/executor/configuration"
)

func Test_ValidateConfig_When_AvoidNodeLabelsOnRetry_MissingFrom_TrackedNodeLabels_Fails(t *testing.T) {
	var config configuration.ExecutorConfiguration
	config.Application.SubmissionThreadCount = 1

	config.Kubernetes.TrackedNodeLabels = []string{"host"}
	config.Kubernetes.AvoidNodeLabelsOnRetry = []string{"host", "cluster"}
	assert.Error(t, validateConfig(config))
}

func Test_ValidateConfig_When_AvoidNodeLabelsOnRetry_And_TrackedNodeLabels_Match_Succeeds(t *testing.T) {
	var config configuration.ExecutorConfiguration
	config.Application.SubmissionThreadCount = 1

	config.Kubernetes.TrackedNodeLabels = []string{"host", "cluster"}
	config.Kubernetes.AvoidNodeLabelsOnRetry = []string{"host", "cluster"}
	assert.NoError(t, validateConfig(config))
}

func Test_ValidateConfig_When_TrackedNodeLabels_MissingFrom_AvoidNodeLabelsOnRetry_Succeeds(t *testing.T) {
	var config configuration.ExecutorConfiguration
	config.Application.SubmissionThreadCount = 1

	config.Kubernetes.TrackedNodeLabels = []string{"host", "cluster"}
	config.Kubernetes.AvoidNodeLabelsOnRetry = []string{"host"}
	assert.NoError(t, validateConfig(config))
}
func Test_ValidateConfig_When_SubmissionThreadCount_GreaterOrEqualToOne(t *testing.T) {
	var config configuration.ExecutorConfiguration
	config.Application.SubmissionThreadCount = 1
	assert.NoError(t, validateConfig(config))

	config.Application.SubmissionThreadCount = 5
	assert.NoError(t, validateConfig(config))
}

func Test_ValidateConfig_When_SubmissionThreadCount_LessThanOrEqualToZero(t *testing.T) {
	var config configuration.ExecutorConfiguration
	config.Application.SubmissionThreadCount = 0
	assert.Error(t, validateConfig(config))

	config.Application.SubmissionThreadCount = -5
	assert.Error(t, validateConfig(config))
}
