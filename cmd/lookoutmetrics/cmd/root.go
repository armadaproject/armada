package cmd

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/lookoutmetrics"
)

const CustomConfigLocation string = "config"

// RootCmd is the root Cobra command that gets called from the main func.
// All other sub-commands should be registered here.
func RootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "lookoutmetrics",
		SilenceUsage: true,
		Short:        "For collecting metrics from the lookoutv2 database.",
	}
	app := lookoutmetrics.New()
	userSpecifiedConfigs := viper.GetStringSlice(CustomConfigLocation)
	common.LoadConfig(&app.Params, "./config/lookoutmetrics", userSpecifiedConfigs)
	cmd.AddCommand(
		runCmd(app),
	)
	return cmd
}

// func initParams(cmd *cobra.Command, app *testsuite.App) error {
// 	if err := client.LoadCommandlineArgs(); err != nil {
// 		return errors.Wrap(err, "error loading command line arguments")
// 	}
// 	app.Params.ApiConnectionDetails = client.ExtractCommandlineArmadaApiConnectionDetails()
// 	return nil
// }

// versionCmd prints version info and exits.
func runCmd(app *lookoutmetrics.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run the app.",
		// PreRunE: func(cmd *cobra.Command, args []string) error {
		// 	return initParams(cmd, app)
		// },
		RunE: func(cmd *cobra.Command, args []string) error {
			return app.Run()
		},
	}
	return cmd
}
