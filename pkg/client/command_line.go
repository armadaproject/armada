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

var defaultArmadaConnectionUrl string = "localhost:50051"

// AddArmadaApiConnectionCommandlineArgs adds command-line flags to a cobra command.
// Arguments given via these flags are later used by LoadCommandlineArgsFromConfigFile.
// Hence, apps that use the client package to load config should call this function as part of
// their initialization.
func AddArmadaApiConnectionCommandlineArgs(rootCmd *cobra.Command) {
	rootCmd.PersistentFlags().StringP("context", "c", "", "specify armada context, as defined in armadactl config")
	err := viper.BindPFlag("currentContext", rootCmd.PersistentFlags().Lookup("context"))
	if err != nil {
		panic(err)
	}

	rootCmd.PersistentFlags().String("armadaUrl", defaultArmadaConnectionUrl, "specify armada server url")
	err = viper.BindPFlag("armadaUrl", rootCmd.PersistentFlags().Lookup("armadaUrl"))
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
		return fmt.Errorf("error finding executable path: %s", err)
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
			return fmt.Errorf("error reading config file %s: %s", viper.ConfigFileUsed(), err)
		}
	} else {
		mergedConfigFiles = append(mergedConfigFiles, viper.ConfigFileUsed())
	}

	// if no cfgFile is provided, use $HOME/.armadactl
	if len(cfgFile) > 0 {
		exists, err := fileutils.IsFile(cfgFile)
		if err != nil {
			return fmt.Errorf("error checking if %s exists: %s", cfgFile, err)
		}
		if !exists {
			return fmt.Errorf("could not find config file %s", cfgFile)
		}
		viper.SetConfigFile(cfgFile)
	} else {
		homeDir, err := homedir.Dir()
		if err != nil {
			return fmt.Errorf("error getting home directory: %s", err)
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
			return fmt.Errorf("error reading config file %s: %s", viper.ConfigFileUsed(), err)
		}
	} else {
		mergedConfigFiles = append(mergedConfigFiles, viper.ConfigFileUsed())
	}

	return nil
}

type CtlConfig struct {
	CurrentContext string                 `yaml:"currentContext"`
	Contexts       map[string]interface{} `yaml:"contexts"`
}

func ReadConfigFromPath(cfgFilePath string) (CtlConfig, error) {
	cfg := CtlConfig{}

	cfgFile, err := os.ReadFile(cfgFilePath)
	if err != nil {
		return cfg, fmt.Errorf("error reading config file: %s", err)
	}

	err = yaml.Unmarshal(cfgFile, &cfg)
	if err != nil {
		return cfg, fmt.Errorf("error unmarshalling config file: %s", err)
	}

	return cfg, nil
}

// ModifyCurrentContextInConfig reads the config file at the provided path, sets the currentContext to the provided
// value, and writes the config back into the file.
func ModifyCurrentContextInConfig(cfgFilePath string, newContext string) error {
	cfg, err := ReadConfigFromPath(cfgFilePath)
	if err != nil {
		return err
	}

	cfg.CurrentContext = newContext

	modifiedCfg, err := yaml.Marshal(&cfg)
	if err != nil {
		return fmt.Errorf("error marshalling config: %s", err)
	}

	err = os.WriteFile(cfgFilePath, modifiedCfg, 0o644)
	if err != nil {
		return fmt.Errorf("error writing modified config: %s", err)
	}

	return nil
}

// SetDefaultContext sets the currentContext within the viper config. We return an error if the context doesn't exist in
// the set of existing contexts
func SetDefaultContext(newContext string) error {
	_, ok := viper.GetStringMap("contexts")[newContext]
	if !ok {
		return fmt.Errorf("could not find context %s in existing contexts", newContext)
	}

	viper.Set("currentContext", newContext)
	return nil
}

// ExtractConfigurationContexts extracts all contexts from loaded viper state
func ExtractConfigurationContexts() []string {
	contexts := []string{}

	for context := range viper.GetStringMap("contexts") {
		contexts = append(contexts, context)
	}

	return contexts
}

// ExtractCommandlineArmadaApiConnectionDetails extracts Armada server connection details from the
// config loaded into viper. Hence, this function must be called after loading config into viper,
// e.g., by calling LoadCommandlineArgsFromConfigFile.
func ExtractCommandlineArmadaApiConnectionDetails() (*ApiConnectionDetails, error) {
	apiConnectionDetails := &ApiConnectionDetails{}
	var err error

	if context := viper.GetString("currentContext"); context != "" {
		subTree := viper.Sub(fmt.Sprintf("contexts.%s", context))
		if subTree == nil {
			return nil, fmt.Errorf("context %s not found under contexts within the Armada config", context)
		}
		err = subTree.Unmarshal(apiConnectionDetails)
		if viper.GetString("armadaUrl") != defaultArmadaConnectionUrl {
			apiConnectionDetails.ArmadaUrl = viper.GetString("armadaUrl")
		}

	} else {
		fmt.Print("No context defined. This method of providing connection details will soon be deprecated, " +
			"the new configuration format is explained here: github.com/armadaproject/armada/blob/master/docs/demo.md#armadactl-configuration\n")
		err = viper.Unmarshal(apiConnectionDetails)
	}

	return apiConnectionDetails, err
}
