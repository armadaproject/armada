package cmd

import (
	log "github.com/sirupsen/logrus"

	"github.com/pkg/errors"

	"github.com/spf13/cobra"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/jobservice"
	"github.com/G-Research/armada/internal/jobservice/configuration"
	"github.com/G-Research/armada/pkg/client"
)

// RootCmd is the root Cobra command that gets called from the main func.
// All other sub-commands should be registered here.
func RootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "jobservice",
		Short: "jobservice is a used for polling functionality",
	}
	client.AddArmadaApiConnectionCommandlineArgs(cmd)
	common.ConfigureLogging()
	common.BindCommandlineArguments()

	var config configuration.JobServiceConfiguration
	common.LoadConfig(&config, "./config/jobservice", []string{})

	cmd.AddCommand(
		runCmd(jobservice.New(&config)),
	)

	return cmd
}
func initParams(cmd *cobra.Command, app *jobservice.App) error {
	if err := client.LoadCommandlineArgs(); err != nil {
		return errors.Wrap(err, "error loading command line arguments")
	}
	log.Info("Extracting CLI from ArmadaCtl")
	app.Config.ApiConnection = *client.ExtractCommandlineArmadaApiConnectionDetails()
	return nil
}
