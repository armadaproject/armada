package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/viper"
)

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.armadactl.yaml)")
	rootCmd.PersistentFlags().String("armadaUrl", "localhost:50051", "specify armada server url")
	rootCmd.PersistentFlags().String("username", "", "username to connect to armada server")
	rootCmd.PersistentFlags().String("password", "", "password to connect to armada server")
	viper.BindPFlag("armadaUrl", rootCmd.PersistentFlags().Lookup("armadaUrl"))
	viper.BindPFlag("username", rootCmd.PersistentFlags().Lookup("username"))
	viper.BindPFlag("password", rootCmd.PersistentFlags().Lookup("password"))
}

var rootCmd = &cobra.Command{
	Use:   "armada-load-tester command",
	Short: "Command line utility to submit many jobs to armada",
	Long:  ``,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

var cfgFile string

// initConfig reads in config file and ENV variables if set.
func initConfig() {
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
