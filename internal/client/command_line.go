package client

import (
	"fmt"
	"os"

	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/G-Research/k8s-batch/internal/client/domain"
	"github.com/G-Research/k8s-batch/internal/common"
)

func AddArmadaApiConnectionCommandlineArgs(rootCmd *cobra.Command) {
	rootCmd.PersistentFlags().String("armadaUrl", "localhost:50051", "specify armada server url")
	rootCmd.PersistentFlags().String("username", "", "username to connect to armada server")
	rootCmd.PersistentFlags().String("password", "", "password to connect to armada server")
	viper.BindPFlag("armadaUrl", rootCmd.PersistentFlags().Lookup("armadaUrl"))
	viper.BindPFlag("username", rootCmd.PersistentFlags().Lookup("username"))
	viper.BindPFlag("password", rootCmd.PersistentFlags().Lookup("password"))
}

func LoadCommandlineArgsFromConfigFile(cfgFile string) {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		viper.AddConfigPath(home)
		viper.SetConfigName(".armadactl")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	err := viper.ReadInConfig()

	if err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	} else {
		switch err.(type) {
		case viper.ConfigFileNotFoundError:
			fmt.Println("No config file:", err)
		default:
			fmt.Println("Can't read config:", err)
			os.Exit(1)
		}
	}
}

func ExtractCommandlineArmadaApiConnectionDetails() *domain.ArmadaApiConnectionDetails {
	url := viper.GetString("armadaUrl")
	username := viper.GetString("username")
	password := viper.GetString("password")

	apiConnectionDetails := domain.ArmadaApiConnectionDetails{
		Url: url,
		Credentials: common.LoginCredentials{
			Username: username,
			Password: password,
		},
	}
	return &apiConnectionDetails
}
