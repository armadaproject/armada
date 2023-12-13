package cmd

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/armadaproject/armada/internal/common"
	commonconfig "github.com/armadaproject/armada/internal/common/config"
	schedulerconfig "github.com/armadaproject/armada/internal/scheduler/configuration"
)

const (
	CustomConfigLocation string = "config"
)

func RootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "scheduler",
		SilenceUsage: true,
		Short:        "The main armada scheduler",
	}

	cmd.PersistentFlags().StringSlice(
		CustomConfigLocation,
		[]string{},
		"Fully qualified path to application configuration file (for multiple config files repeat this arg or separate paths with commas)")
	err := viper.BindPFlag(CustomConfigLocation, cmd.PersistentFlags().Lookup(CustomConfigLocation))
	if err != nil {
		panic(err)
	}
	cmd.AddCommand(
		runCmd(),
		migrateDbCmd(),
		pruneDbCmd(),
	)

	return cmd
}

func loadConfig() (schedulerconfig.Configuration, error) {
	var config schedulerconfig.Configuration
	common.LoadConfig(&config, "./config/scheduler", viper.GetStringSlice(CustomConfigLocation))
	err := config.Validate()
	if err != nil {
		commonconfig.LogValidationErrors(err)
	}
	return config, err
}
