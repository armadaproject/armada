package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/G-Research/armada/internal/armadactl"
)

func init() {
	rootCmd.AddCommand(watchCmd())
}

func watchCmd() *cobra.Command {
	a := armadactl.New()
	cmd := &cobra.Command{
		Use:   "watch <queue> <jobSet>",
		Short: "Watch job events in job set.",
		Long:  `This command will list all job set events and `,
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

			exit_on_inactive, err := cmd.Flags().GetBool("exit-if-inactive")
			if err != nil {
				return fmt.Errorf("error reading exit-if-inactive: %s", err)
			}

			return a.Watch(queue, jobSetId, raw, exit_on_inactive)
		},
	}
	cmd.Flags().Bool("raw", false, "Output raw events")
	cmd.Flags().Bool("exit-if-inactive", false, "Exit if there are no more active jobs")
	return cmd
}
