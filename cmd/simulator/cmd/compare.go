package cmd

import (
	"errors"
	"os"
	"os/signal"
	"time"

	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/internal/scheduler/simulator/replay"
)

func compareCmd() *cobra.Command {
	var manifest, output string
	var timeout time.Duration
	cmd := &cobra.Command{
		Use:   "compare",
		Short: "Compare local Armada scheduling candidates on the same workload",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if manifest == "" || output == "" || timeout <= 0 {
				return errors.New("compare requires --experiment, --output and a positive --timeout")
			}
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt)
			defer stop()
			return replay.Compare(ctx, manifest, output, timeout, cmd.OutOrStdout())
		},
	}
	cmd.Flags().StringVar(&manifest, "experiment", "", "Versioned comparison JSON file")
	cmd.Flags().StringVar(&output, "output", "", "New directory for the comparison and candidate results")
	cmd.Flags().DurationVar(&timeout, "timeout", 30*time.Second, "Maximum elapsed time per candidate")
	return cmd
}

func compareCandidateCmd() *cobra.Command {
	return &cobra.Command{
		Use:    "compare-candidate INPUT RESULT",
		Hidden: true,
		Args:   cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return replay.RunComparisonCandidate(cmd.Context(), args[0], args[1])
		},
	}
}
