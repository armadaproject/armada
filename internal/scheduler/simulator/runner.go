package simulator

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/mattn/go-zglob"
	"github.com/pkg/errors"
	"github.com/renstrom/shortuuid"
	"github.com/spf13/viper"
	"k8s.io/apimachinery/pkg/util/yaml"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	commonconfig "github.com/armadaproject/armada/internal/common/config"
)

func Simulate(ctx *armadacontext.Context, clusterSpecsPattern, workloadSpecsPattern, schedulingConfigsPattern string) error {
	clusterSpecs, err := ClusterSpecsFromPattern(clusterSpecsPattern)
	if err != nil {
		return err
	}
	workloadSpecs, err := WorkloadFromPattern(workloadSpecsPattern)
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
		return config, errors.WithStack(err)
	}
	if err := v.Unmarshal(&config, commonconfig.CustomHooks...); err != nil {
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

func WorkloadFromPattern(pattern string) ([]*WorkloadSpec, error) {
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
	yamlBytes, err := os.ReadFile(filePath)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if len(yamlBytes) == 0 {
		return nil, errors.Errorf("%s does not exist or is empty", filePath)
	}
	var clusterSpec ClusterSpec
	if err := unmarshalYamlBytes(yamlBytes, &clusterSpec); err != nil {
		return nil, err
	}

	// If no test name is provided, set it to be the filename.
	if clusterSpec.Name == "" {
		fileName := filepath.Base(filePath)
		fileName = strings.TrimSuffix(fileName, filepath.Ext(fileName))
		clusterSpec.Name = fileName
	}
	initialiseClusterSpec(&clusterSpec)

	return &clusterSpec, nil
}

func WorkloadSpecFromFilePath(filePath string) (*WorkloadSpec, error) {
	yamlBytes, err := os.ReadFile(filePath)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if len(yamlBytes) == 0 {
		return nil, errors.Errorf("%s does not exist or is empty", filePath)
	}
	var workloadSpec WorkloadSpec
	if err := unmarshalYamlBytes(yamlBytes, &workloadSpec); err != nil {
		return nil, err
	}

	// If no test name is provided, set it to be the filename.
	if workloadSpec.Name == "" {
		fileName := filepath.Base(filePath)
		fileName = strings.TrimSuffix(fileName, filepath.Ext(fileName))
		workloadSpec.Name = fileName
	}

	// Generate random ids for any job templates without an explicitly set id.
	for _, queue := range workloadSpec.Queues {
		for j, jobTemplate := range queue.JobTemplates {
			if jobTemplate.Id == "" {
				jobTemplate.Id = shortuuid.New()
			}
			queue.JobTemplates[j] = jobTemplate
		}
	}
	initialiseWorkloadSpec(&workloadSpec)

	return &workloadSpec, nil
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

func unmarshalYamlBytes(yamlBytes []byte, dst any) error {
	if err := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(yamlBytes), 128).Decode(dst); err != nil {
		return errors.WithStack(err)
	}
	return nil
}
