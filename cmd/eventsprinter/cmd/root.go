package cmd

import (
	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/cmd/eventsprinter/logic"
)

// RootCmd is the root Cobra command that gets called from the main func.
// All other sub-commands should be registered here.
func RootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "eventsprinter",
		Short: "eventsprinter prints all Pulsar events",
		RunE: func(cmd *cobra.Command, args []string) error {
			url, err := cmd.PersistentFlags().GetString("url")
			if err != nil {
				return err
			}
			verbose, err := cmd.PersistentFlags().GetBool("verbose")
			if err != nil {
				return err
			}
			subscription, err := cmd.PersistentFlags().GetString("subscription")
			if err != nil {
				return err
			}
			topic, err := cmd.PersistentFlags().GetString("topic")
			if err != nil {
				return err
			}
			return logic.PrintEvents(url, topic, subscription, verbose)
		},
	}
	cmd.PersistentFlags().String("url", "pulsar://localhost:6650", "URL to connect to Pulsar on.")
	cmd.PersistentFlags().Bool("verbose", false, "Print full event sequences.")
	cmd.PersistentFlags().String("subscription", "eventsprinter", "Subscription to connect to Pulsar on.")
	cmd.PersistentFlags().String("topic", "events", "Pulsar topic to subscribe to.")

	return cmd
}
