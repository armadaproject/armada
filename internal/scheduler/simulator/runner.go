package simulator

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/mattn/go-zglob"
	"github.com/pkg/errors"
	"github.com/renstrom/shortuuid"
	"github.com/spf13/viper"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	commonconfig "github.com/armadaproject/armada/internal/common/config"
)

func Simulate(ctx *armadacontext.Context, clusterSpecsPattern, workloadSpecsPattern, schedulingConfigsPattern string) error {
	clusterSpecs, err := ClusterSpecsFromPattern(clusterSpecsPattern)
	if err != nil {
		return err
	}
	workloadSpecs, err := WorkloadsFromPattern(workloadSpecsPattern)
	if err != nil {
		return err
	}
	schedulingConfigs, err := SchedulingConfigsFromPattern(schedulingConfigsPattern)
	if err != nil {
		return err
	}
	g, ctx := armadacontext.ErrGroup(ctx)
	for _, clusterSpec := range clusterSpecs {
		for _, workloadSpec := range workloadSpecs {
			for _, schedulingConfig := range schedulingConfigs {
				s, err := NewSimulator(clusterSpec, workloadSpec, schedulingConfig)
				if err != nil {
					return err
				}
				g.Go(func() error {
					return s.Run(ctx)
				})
			}
		}
	}
	return g.Wait()
}

func SchedulingConfigsByFilePathFromPattern(pattern string) (map[string]configuration.SchedulingConfig, error) {
	filePaths, err := zglob.Glob(pattern)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	filePathConfigMap := make(map[string]configuration.SchedulingConfig)
	for _, path := range filePaths {
		config, err := SchedulingConfigsFromFilePaths(filePaths)
		if err != nil {
			return nil, err
		}
		filePathConfigMap[path] = config[0]
	}
	return filePathConfigMap, nil
}

func SchedulingConfigsFromPattern(pattern string) ([]configuration.SchedulingConfig, error) {
	filePaths, err := zglob.Glob(pattern)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return SchedulingConfigsFromFilePaths(filePaths)
}

func SchedulingConfigsFromFilePaths(filePaths []string) ([]configuration.SchedulingConfig, error) {
	rv := make([]configuration.SchedulingConfig, len(filePaths))
	for i, filePath := range filePaths {
		config, err := SchedulingConfigFromFilePath(filePath)
		if err != nil {
			return nil, err
		}
		rv[i] = config
	}
	return rv, nil
}

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

func ClusterSpecsFromPattern(pattern string) ([]*ClusterSpec, error) {
	filePaths, err := zglob.Glob(pattern)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return ClusterSpecsFromFilePaths(filePaths)
}

func WorkloadsFromPattern(pattern string) ([]*WorkloadSpec, error) {
	filePaths, err := zglob.Glob(pattern)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return WorkloadSpecsFromFilePaths(filePaths)
}

func ClusterSpecsFromFilePaths(filePaths []string) ([]*ClusterSpec, error) {
	rv := make([]*ClusterSpec, len(filePaths))
	for i, filePath := range filePaths {
		clusterSpec, err := ClusterSpecFromFilePath(filePath)
		if err != nil {
			return nil, err
		}
		rv[i] = clusterSpec
	}
	return rv, nil
}

func WorkloadSpecsFromFilePaths(filePaths []string) ([]*WorkloadSpec, error) {
	rv := make([]*WorkloadSpec, len(filePaths))
	for i, filePath := range filePaths {
		workloadSpec, err := WorkloadSpecFromFilePath(filePath)
		if err != nil {
			return nil, err
		}
		rv[i] = workloadSpec
	}
	return rv, nil
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
	initialiseClusterSpec(rv)

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

	// Generate random ids for any job templates without an explicitly set id.
	for _, queue := range rv.Queues {
		for j, jobTemplate := range queue.JobTemplates {
			if jobTemplate.Id == "" {
				jobTemplate.Id = shortuuid.New()
			}
			queue.JobTemplates[j] = jobTemplate
		}
	}
	initialiseWorkloadSpec(rv)

	return rv, nil
}

func initialiseClusterSpec(clusterSpec *ClusterSpec) {
	// Assign names to executors with none specified.
	for _, pool := range clusterSpec.Pools {
		for i, executorGroup := range pool.ClusterGroups {
			for j, executor := range executorGroup.Clusters {
				if executor.Name == "" {
					executor.Name = fmt.Sprintf("%s-%d-%d", pool.Name, i, j)
				}
			}
		}
	}
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
