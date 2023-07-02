package cmd

import (
	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/internal/testsuite"
	"github.com/armadaproject/armada/pkg/client"
)

// RootCmd is the root Cobra command that gets called from the main func.
// All other sub-commands should be registered here.
func RootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "testsuite",
		SilenceUsage: true,
		Short:        "testsuite is a suite of automated tests for Armada deployments.",
		Long: `testsuite is a suite of automated tests for Armada deployments.

Persistent config can be saved in a config file so it doesn't have to be specified every command.
The location of this file can be passed in using the --config argument.
If not provided, $HOME/.armadactl.yaml is used.`,
	}

	client.AddArmadaApiConnectionCommandlineArgs(cmd)

	cmd.AddCommand(
		versionCmd(testsuite.New()),
		testCmd(testsuite.New()),
	)

	return cmd
}

func initParams(cmd *cobra.Command, app *testsuite.App) error {
	if err := client.LoadCommandlineArgs(); err != nil {
		return errors.Wrap(err, "error loading command line arguments")
	}
	app.Params.ApiConnectionDetails = client.ExtractCommandlineArmadaApiConnectionDetails()
	return nil
}
