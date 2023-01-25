package cmd

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/armadaproject/armada/internal/common"
	commonconfig "github.com/armadaproject/armada/internal/common/config"
	"github.com/armadaproject/armada/internal/scheduler"
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
		"armadaUrl",
		[]string{},
		"Fully qualified path to application configuration file (for multiple config files repeat this arg or separate paths with commas)")

	cmd.AddCommand(
		runCmd(),
		migrateDbCmd(),
	)

	return cmd
}

func loadConfig() (scheduler.Configuration, error) {
	var config scheduler.Configuration
	userSpecifiedConfigs := viper.GetStringSlice(CustomConfigLocation)

	common.LoadConfig(&config, "./config/scheduler", userSpecifiedConfigs)

	// TODO: once we're happy with this we can move it to common app startup
	err := commonconfig.Validate(config)
	if err != nil {
		commonconfig.LogValidationErrors(err)
	}
	return config, err
}
