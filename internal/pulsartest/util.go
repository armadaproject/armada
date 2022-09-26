package pulsartest

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/G-Research/armada/internal/fileutils"
	"github.com/G-Research/armada/pkg/armadaevents"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/hashicorp/go-multierror"
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/viper"
	apimachineryYaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"
)

// LoadCommandlineArgsFromConfigFile loads pulsartest config
// pulsartest-defaults.yaml - From exePath, where exePath is the path to the pulsartest executable
// armada config file - From cfgFile or defaulting to $HOME/.pulsartest
// These configs are then merged
func LoadCommandlineArgsFromConfigFile(cfgFile string) ([]string, error) {
	var mergedConfigFiles []string // all config files that have been loaded

	// read config at exePath/pulsartest-defaults.yaml, where exePath is the path to the pulsartest
	// executable (if it exists)
	exePath, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("[LoadCommandlineArgsFromConfigFile] error finding executable path: %s", err)
	}

	exeDir := filepath.Dir(exePath)
	configPath := filepath.Join(exeDir, "/pulsartest-defaults.yaml")
	viper.SetConfigFile(configPath)
	if err := viper.ReadInConfig(); err != nil {
		switch err.(type) {
		case viper.ConfigFileNotFoundError:
		case *os.PathError:
			// Config file not found; ignore
		default:
			return nil, fmt.Errorf("[LoadCommandlineArgsFromConfigFile] error reading config file %s: %s", viper.ConfigFileUsed(), err)
		}
	} else {
		mergedConfigFiles = append(mergedConfigFiles, viper.ConfigFileUsed())
	}

	// if no cfgFile is provided, use $HOME/.pulsartest
	if len(cfgFile) > 0 {
		exists, err := fileutils.IsFile(cfgFile)
		if err != nil {
			return nil, fmt.Errorf("[LoadCommandlineArgsFromConfigFile] error checking if %s exists: %s", cfgFile, err)
		}
		if !exists {
			return nil, fmt.Errorf("[LoadCommandlineArgsFromConfigFile] could not find config file %s", cfgFile)
		}
		viper.SetConfigFile(cfgFile)
	} else {
		homeDir, err := homedir.Dir()
		if err != nil {
			return nil, fmt.Errorf("[LoadCommandlineArgsFromConfigFile] error getting home directory: %s", err)
		}
		viper.AddConfigPath(homeDir)
		viper.SetConfigName(".pulsartest")
	}

	// read environment variables
	viper.AutomaticEnv()

	// merge in new config with those loaded from pulsartest-defaults.yaml
	// (note the call to viper.MergeInConfig instead of viper.ReadInConfig)
	if err := viper.ReadInConfig(); err != nil {
		switch err.(type) {
		case viper.ConfigFileNotFoundError:
		case *os.PathError:
			// Config file not found; ignore
		default:
			return nil, fmt.Errorf("[LoadCommandlineArgsFromConfigFile] error reading config file %s: %s", viper.ConfigFileUsed(), err)
		}
	} else {
		mergedConfigFiles = append(mergedConfigFiles, viper.ConfigFileUsed())
	}

	return mergedConfigFiles, nil
}

// UnmarshalEventSubmission unmarshalls bytes into an EventSequence
func UnmarshalEventSubmission(yamlBytes []byte, es *armadaevents.EventSequence) error {
	var result *multierror.Error
	successExpectedEvents := false
	successEverythingElse := false
	yamlSpecSeparator := []byte("---")
	docs := bytes.Split(yamlBytes, yamlSpecSeparator)
	for _, docYamlBytes := range docs {

		// yaml.Unmarshal can unmarshal everything,
		// but leaves oneof fields empty (these are silently discarded).
		if err := apimachineryYaml.NewYAMLOrJSONDecoder(bytes.NewReader(yamlBytes), 128).Decode(es); err != nil {
			result = multierror.Append(result, err)
		} else {
			successEverythingElse = true
		}

		// YAMLToJSON + jsonpb.Unmarshaler can unmarshal oneof fields,
		// but can't unmarshal k8s pod specs.
		docJsonBytes, err := yaml.YAMLToJSON(docYamlBytes)
		if err != nil {
			result = multierror.Append(result, err)
			continue
		}
		unmarshaler := jsonpb.Unmarshaler{AllowUnknownFields: true}
		err = unmarshaler.Unmarshal(bytes.NewReader(docJsonBytes), es)
		if err != nil {
			result = multierror.Append(result, err)
		} else {
			successExpectedEvents = true
		}
	}
	if !successExpectedEvents || !successEverythingElse {
		return result.ErrorOrNil()
	}
	return nil
}
