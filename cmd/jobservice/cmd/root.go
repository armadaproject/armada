package cmd

import (
	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/jobservice"
)

// RootCmd is the root Cobra command that gets called from the main func.
// All other sub-commands should be registered here.
func RootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "jobservice",
		Short: "jobservice is used for polling functionality",
	}
	common.ConfigureLogging()
	common.BindCommandlineArguments()

	cmd.AddCommand(
		runCmd(jobservice.New()),
	)

	return cmd
}
