package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/internal/armadactl"
)

func watchCmd() *cobra.Command {
	a := armadactl.New()
	cmd := &cobra.Command{
		Use:   "watch <queue> <jobSet>",
		Short: "Watch job events in job set.",
		Long:  "Listens for and prints events associated with a particular queue and jobset.",
		Args:  cobra.ExactArgs(2),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			queue := args[0]
			jobSetId := args[1]

			raw, err := cmd.Flags().GetBool("raw")
			if err != nil {
				return fmt.Errorf("error reading raw: %s", err)
			}

			exitOnInactive, err := cmd.Flags().GetBool("exit-if-inactive")
			if err != nil {
				return fmt.Errorf("error reading exit-if-inactive: %s", err)
			}

			forceNewEvents, err := cmd.Flags().GetBool("force-new-events")
			if err != nil {
				return fmt.Errorf("error reading force-new-events: %s", err)
			}

			forceLegacyEvents, err := cmd.Flags().GetBool("force-legacy-events")
			if err != nil {
				return fmt.Errorf("error reading force-legacy-events: %s", err)
			}

			if forceNewEvents && forceLegacyEvents {
				return fmt.Errorf("force-new-events and force-legacy-events are exclusive")
			}

			return a.Watch(queue, jobSetId, raw, exitOnInactive, forceNewEvents, forceLegacyEvents)
		},
	}
	cmd.Flags().Bool("raw", false, "Output raw events")
	cmd.Flags().Bool("exit-if-inactive", false, "Exit if there are no more active jobs")
	cmd.Flags().Bool("force-new-events", false, "Debug Option to tell Armada server to serve events from the new redis repository")
	cmd.Flags().Bool("force-legacy-events", false, "Debug Option to tell Armada server to serve events from the old redis repository")
	return cmd
}
