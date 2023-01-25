package cmd

import (
	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/internal/armadactl"
	"github.com/armadaproject/armada/pkg/client"
)

// RootCmd is the root Cobra command that gets called from the main func.
// All other sub-commands should be registered here.
func RootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "armadactl",
		Short: "armadactl controls the Armada batch job queueing system.",
		Long: `armadactl controls the Armada batch job queueing system.
	
Persistent config can be saved in a config file so it doesn't have to be specified every command.

Example structure:
armadaUrl: localhost:50051
basicAuth:
username: user1
password: password123

The location of this file can be passed in using --config argument or picked from $HOME/.armadactl.yaml.`,
	}

	client.AddArmadaApiConnectionCommandlineArgs(cmd)

	cmd.AddCommand(
		analyzeCmd(),
		cancelCmd(),
		createCmd(armadactl.New()),
		deleteCmd(),
		updateCmd(),
		describeCmd(),
		kubeCmd(),
		reprioritizeCmd(),
		resourcesCmd(),
		submitCmd(),
		versionCmd(),
		watchCmd(),
		getQueueSchedulingReportCmd(armadactl.New()),
		getJobSchedulingReportCmd(armadactl.New()),
	)

	return cmd
}
