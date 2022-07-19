package cmd

import (
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
		Short: "jobservice is used for polling functionality",
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
