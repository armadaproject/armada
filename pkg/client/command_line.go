package client

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v2"

	"github.com/armadaproject/armada/internal/common/fileutils"
)

// list of config files loaded into viper
// is appended to manually from within LoadCommandlineArgsFromConfigFile
var mergedConfigFiles []string

// path to config file, as given by viper flags
var cfgFile string

// AddArmadaApiConnectionCommandlineArgs adds command-line flags to a cobra command.
// Arguments given via these flags are later used by LoadCommandlineArgsFromConfigFile.
// Hence, apps that use the client package to load config should call this function as part of
// their initialization.
func AddArmadaApiConnectionCommandlineArgs(rootCmd *cobra.Command) {
	rootCmd.PersistentFlags().String("armadaUrl", "localhost:50051", "specify armada server url")
	err := viper.BindPFlag("armadaUrl", rootCmd.PersistentFlags().Lookup("armadaUrl"))
	if err != nil {
		panic(err)
	}

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.armadactl.yaml)")
}

// LoadCommandlineArgs loads armadactl config
// armadactl-defaults.yaml - From exePath, where exePath is the path to the armadactl executable
// armada config file - From cfgFile, set by the --config CLI flag, or defaulting to $HOME/.armadactl if not set
// These configs are then merged
func LoadCommandlineArgs() error {
	return LoadCommandlineArgsFromConfigFile(cfgFile)
}

// LoadCommandlineArgsFromConfigFile loads armadactl config
// armadactl-defaults.yaml - From exePath, where exePath is the path to the armadactl executable
// armada config file - From cfgFile or defaulting to $HOME/.armadactl
// These configs are then merged
func LoadCommandlineArgsFromConfigFile(cfgFile string) error {
	// read config at exePath/armadactl-defaults.yaml, where exePath is the path to the armadactl
	// executable (if it exists)
	exePath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("[LoadCommandlineArgsFromConfigFile] error finding executable path: %s", err)
	}

	exeDir := filepath.Dir(exePath)
	configPath := filepath.Join(exeDir, "/armadactl-defaults.yaml")
	viper.SetConfigFile(configPath)
	if err := viper.ReadInConfig(); err != nil {
		switch err.(type) {
		case viper.ConfigFileNotFoundError:
		case *os.PathError:
			// Config file not found; ignore
		default:
			return fmt.Errorf("[LoadCommandlineArgsFromConfigFile] error reading config file %s: %s", viper.ConfigFileUsed(), err)
		}
	} else {
		mergedConfigFiles = append(mergedConfigFiles, viper.ConfigFileUsed())
	}

	// if no cfgFile is provided, use $HOME/.armadactl
	if len(cfgFile) > 0 {
		exists, err := fileutils.IsFile(cfgFile)
		if err != nil {
			return fmt.Errorf("[LoadCommandlineArgsFromConfigFile] error checking if %s exists: %s", cfgFile, err)
		}
		if !exists {
			return fmt.Errorf("[LoadCommandlineArgsFromConfigFile] could not find config file %s", cfgFile)
		}
		viper.SetConfigFile(cfgFile)
	} else {
		homeDir, err := homedir.Dir()
		if err != nil {
			return fmt.Errorf("[LoadCommandlineArgsFromConfigFile] error getting home directory: %s", err)
		}
		viper.AddConfigPath(homeDir)
		viper.SetConfigName(".armadactl")
	}

	// read environment variables
	viper.AutomaticEnv()

	// merge in new config with those loaded from armadactl-defaults.yaml
	// (note the call to viper.MergeInConfig instead of viper.ReadInConfig)
	if err := viper.ReadInConfig(); err != nil {
		switch err.(type) {
		case viper.ConfigFileNotFoundError:
		case *os.PathError:
			// Config file not found; ignore
		default:
			return fmt.Errorf("[LoadCommandlineArgsFromConfigFile] error reading config file %s: %s", viper.ConfigFileUsed(), err)
		}
	} else {
		mergedConfigFiles = append(mergedConfigFiles, viper.ConfigFileUsed())
	}

	return nil
}

// ExtractCommandlineArmadaApiConnectionDetails extracts Armada server connection details from the
// config loaded into viper. Hence, this function must be called after loading config into viper,
// e.g., by calling LoadCommandlineArgsFromConfigFile.
func ExtractCommandlineArmadaApiConnectionDetails() *ApiConnectionDetails {
	apiConnectionDetails := &ApiConnectionDetails{}
	err := viper.Unmarshal(apiConnectionDetails)
	if err != nil {
		panic(err)
	}
	return apiConnectionDetails
}

type armadaClientConfig struct {
	ApiVersion     string
	Users          map[string]*userConfig
	Clusters       map[string]*clusterConfig
	Contexts       map[string]*contextConfig
	CurrentContext string
}

type userConfig struct {
	Username string
	Password string
}

type clusterConfig struct {
	Server                string
	InsecureSkipTlsVerify bool
}

type contextConfig struct {
	Cluster string
	User    string
}

// validate checks that no names are the empty string, that users/clusters references by the
// contexts exists, and that the current context is defined. Other methods on armadaClientConfig
// are themselves responsible for performing any further validation.
func (config *armadaClientConfig) validate() error {
	if config == nil {
		return fmt.Errorf("[armadaClientConfig.validate] config is nil")
	}

	if config.Users != nil {
		for name, user := range config.Users {
			if name == "" {
				return fmt.Errorf("[armadaClientConfig.validate] empty user names not allowed")
			}
			if user == nil {
				return fmt.Errorf("[armadaClientConfig.validate] user %s is nil", name)
			}
		}
	}

	if config.Clusters != nil {
		for name, cluster := range config.Clusters {
			if name == "" {
				return fmt.Errorf("[armadaClientConfig.validate] empty cluster names not allowed")
			}
			if cluster == nil {
				return fmt.Errorf("[armadaClientConfig.validate] cluster %s is nil", name)
			}
		}
	}

	if config.Contexts != nil {
		if config.Users == nil {
			return fmt.Errorf("[armadaClientConfig.validate] no users provided")
		}
		if config.Clusters == nil {
			return fmt.Errorf("[armadaClientConfig.validate] no clusters provided")
		}

		for name, context := range config.Contexts {
			if name == "" {
				return fmt.Errorf("[armadaClientConfig.validate] empty context names not allowed")
			}
			if context == nil {
				return fmt.Errorf("[armadaClientConfig.validate] context %s is nil", name)
			}
			if _, ok := config.Clusters[context.Cluster]; !ok {
				return fmt.Errorf("[armadaClientConfig.validate] could not find cluster %s referenced by context %s", context.Cluster, name)
			}
			if _, ok := config.Users[context.User]; !ok {
				return fmt.Errorf("[armadaClientConfig.validate] could not find user %s referenced by context %s", context.User, name)
			}
		}
	}

	if config.CurrentContext != "" {
		if config.Clusters == nil {
			return fmt.Errorf("[armadaClientConfig.validate] could not find current context %s", config.CurrentContext)
		}
		if _, ok := config.Contexts[config.CurrentContext]; !ok {
			return fmt.Errorf("[armadaClientConfig.validate] could not find current context %s", config.CurrentContext)
		}
	}

	return nil
}

// ConfigYamlString returns a string representation of the merged configuration stored in viper.
func (config *armadaClientConfig) YamlString() (string, error) {
	bs, err := yaml.Marshal(config)
	if err != nil {
		return "", fmt.Errorf("[armadaClientConfig.YamlString] unable to marshal config to yaml: %s", err)
	}
	return string(bs), nil
}

// LoadClientConfig reads in config from the file at configPath, or, if configPath is the empty
// string, from $HOME/.armada/config, and from environment variables, using viper.
func LoadClientConfig(configPath string) (*armadaClientConfig, error) {
	// if no file is provided (i.e., if configPath is the empty string), default to $HOME/.armada/config
	if len(configPath) == 0 {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("[LoadClientConfig] error getting user home directory: %s", err)
		}
		configPath = filepath.Join(homeDir, "/.armada/", "config")
	}

	// read config from file; ignore if it doesn't exist
	viper.SetConfigType("yaml")
	viper.SetConfigFile(configPath)
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// Config file not found; ignore
		} else {
			return nil, fmt.Errorf("[LoadClientConfig] error reading config file %s: %s", viper.ConfigFileUsed(), err)
		}
	}

	// read environment variables
	viper.AutomaticEnv()

	// read config into an armadaConfig struct
	// var config armadaClientConfig
	config := &armadaClientConfig{}
	if err := viper.Unmarshal(config); err != nil {
		return nil, err
	}

	if err := config.validate(); err != nil {
		return nil, fmt.Errorf("[LoadClientConfig] error validating config: %s", err)
	}

	return config, nil
}

// CurrentApiConnectionDetails returns connection details for the currently active cluster server,
// as specified by the currentContext setting.
func (config *armadaClientConfig) CurrentApiConnectionDetails() (*ApiConnectionDetails, error) {
	return nil, fmt.Errorf("[armadaClientConfig.CurrentApiConnectionDetails] error: not implemented")
}
