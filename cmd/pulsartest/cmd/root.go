package cmd

import (
	"github.com/spf13/cobra"
)

// RootCmd is the root Cobra command that gets called from the main func.
// All other sub-commands should be registered here.
func RootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pulsartest",
		Short: "pulsartest provides tools for generating and observing Pulsar events.",
	}

	cmd.PersistentFlags().String("url", "pulsar://localhost:6650", "URL to connect to Pulsar on.")
	cmd.PersistentFlags().Bool("authenticationEnabled", false, "Use authentication.")
	cmd.PersistentFlags().String("authenticationType", "JWT", "Authentication type")
	cmd.PersistentFlags().String("jwtTokenPath", "", "Path of JWT file")
	cmd.PersistentFlags().String("jobsetEventsTopic", "events", "Pulsar topic for this jobset")
	cmd.PersistentFlags().String("compressionType", "none", "Type of compression to use")

	cmd.AddCommand(
		submitCmd(),
		watchCmd(),
	)

	return cmd
}
