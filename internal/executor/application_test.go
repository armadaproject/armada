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

func TestValidateConfig_When_FractionStorageInUseSoftLimit_LessThan0(t *testing.T) {
	config := createBasicValidExecutorConfiguration()

	config.Kubernetes.Etcd.FractionOfStorageInUseSoftLimit = 0
	assert.Error(t, validateConfig(config))
	config.Kubernetes.Etcd.FractionOfStorageInUseSoftLimit = -1
	assert.Error(t, validateConfig(config))
}

func TestValidateConfig_When_FractionStorageInUseSoftLimit_GreaterThan1(t *testing.T) {
	config := createBasicValidExecutorConfiguration()

	config.Kubernetes.Etcd.FractionOfStorageInUseSoftLimit = 1
	assert.NoError(t, validateConfig(config))
	config.Kubernetes.Etcd.FractionOfStorageInUseSoftLimit = 1.1
	assert.Error(t, validateConfig(config))
}

func TestValidateConfig_When_FractionStorageInUseHardLimit_LessThan0(t *testing.T) {
	config := createBasicValidExecutorConfiguration()

	config.Kubernetes.Etcd.FractionOfStorageInUseHardLimit = 0
	assert.Error(t, validateConfig(config))
	config.Kubernetes.Etcd.FractionOfStorageInUseHardLimit = -1
	assert.Error(t, validateConfig(config))
}

func TestValidateConfig_When_FractionStorageInUseHardLimit_GreaterThan1(t *testing.T) {
	config := createBasicValidExecutorConfiguration()

	config.Kubernetes.Etcd.FractionOfStorageInUseHardLimit = 1
	assert.NoError(t, validateConfig(config))
	config.Kubernetes.Etcd.FractionOfStorageInUseHardLimit = 1.1
	assert.Error(t, validateConfig(config))
}

func createBasicValidExecutorConfiguration() configuration.ExecutorConfiguration {
	return configuration.ExecutorConfiguration{
		Application: configuration.ApplicationConfiguration{
			SubmitConcurrencyLimit: 1,
			UpdateConcurrencyLimit: 1,
			DeleteConcurrencyLimit: 1,
		},
		Kubernetes: configuration.KubernetesConfiguration{
			Etcd: configuration.EtcdConfiguration{
				FractionOfStorageInUseSoftLimit: 0.8,
				FractionOfStorageInUseHardLimit: 0.9,
			},
		},
	}
}
