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
	}

	client.AddArmadaApiConnectionCommandlineArgs(cmd)

	cmd.AddCommand(
		cancelCmd(),
		createCmd(armadactl.New()),
		deleteCmd(),
		updateCmd(),
		getCmd(),
		reprioritizeCmd(),
		submitCmd(),
		versionCmd(),
		watchCmd(),
		configCmd(armadactl.New()),
		preemptCmd(),
		docsCmd(),
		cordon(),
		uncordon(),
	)

	return cmd
}
