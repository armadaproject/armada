package simulator

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/viper"

	commonconfig "github.com/armadaproject/armada/internal/common/config"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
)

func SchedulingConfigFromFilePath(filePath string) (configuration.SchedulingConfig, error) {
	config := configuration.SchedulingConfig{}
	v := viper.NewWithOptions(viper.KeyDelimiter("::"))
	v.SetConfigFile(filePath)
	if err := v.ReadInConfig(); err != nil {
		err = errors.WithMessagef(err, "failed to read in SchedulingConfig %s", filePath)
		return config, errors.WithStack(err)
	}
	if err := v.Unmarshal(&config, commonconfig.CustomHooks...); err != nil {
		err = errors.WithMessagef(err, "failed to unmarshal SchedulingConfig %s", filePath)
		return config, errors.WithStack(err)
	}
	return config, nil
}

func ClusterSpecFromFilePath(filePath string) (*ClusterSpec, error) {
	rv := &ClusterSpec{}
	v := viper.NewWithOptions(viper.KeyDelimiter("::"))
	v.SetConfigFile(filePath)
	if err := v.ReadInConfig(); err != nil {
		err = errors.WithMessagef(err, "failed to read in ClusterSpec %s", filePath)
		return nil, errors.WithStack(err)
	}
	if err := v.Unmarshal(rv, commonconfig.CustomHooks...); err != nil {
		err = errors.WithMessagef(err, "failed to unmarshal ClusterSpec %s", filePath)
		return nil, errors.WithStack(err)
	}

	// If no test name is provided, set it to be the filename.
	if rv.Name == "" {
		fileName := filepath.Base(filePath)
		fileName = strings.TrimSuffix(fileName, filepath.Ext(fileName))
		rv.Name = fileName
	}

	return rv, nil
}

func WorkloadSpecFromFilePath(filePath string) (*WorkloadSpec, error) {
	rv := &WorkloadSpec{}
	v := viper.NewWithOptions(viper.KeyDelimiter("::"))
	v.SetConfigFile(filePath)
	if err := v.ReadInConfig(); err != nil {
		err = errors.WithMessagef(err, "failed to read in WorkloadSpec %s", filePath)
		return nil, errors.WithStack(err)
	}
	if err := v.Unmarshal(rv, commonconfig.CustomHooks...); err != nil {
		err = errors.WithMessagef(err, "failed to unmarshal WorkloadSpec %s", filePath)
		return nil, errors.WithStack(err)
	}

	// If no test name is provided, set it to be the filename.
	if rv.Name == "" {
		fileName := filepath.Base(filePath)
		fileName = strings.TrimSuffix(fileName, filepath.Ext(fileName))
		rv.Name = fileName
	}

	initialiseWorkloadSpec(rv)
	return rv, nil
}

func initialiseWorkloadSpec(workloadSpec *WorkloadSpec) {
	// Assign names to jobTemplates with none specified.
	for _, queue := range workloadSpec.Queues {
		for i, jobTemplate := range queue.JobTemplates {
			if jobTemplate.Id == "" {
				jobTemplate.Id = fmt.Sprintf("%s-%d", queue.Name, i)
			}
			jobTemplate.Queue = queue.Name
		}
	}
}
