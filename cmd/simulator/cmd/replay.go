package cmd

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"time"

	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/internal/scheduler/simulator/replay"
)

func replayCmd() *cobra.Command {
	var scenario, output, patch string
	var timeout time.Duration
	cmd := &cobra.Command{
		Use:   "replay",
		Short: "Replay a local recorded workload through the Armada scheduler",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if scenario == "" || output == "" || timeout <= 0 {
				return errors.New("replay requires --scenario, --output and a positive --timeout")
			}
			signals, stop := signal.NotifyContext(cmd.Context(), os.Interrupt)
			defer stop()
			ctx, cancel := context.WithTimeout(signals, timeout)
			defer cancel()
			return replay.RunScenario(ctx, scenario, output, patch)
		},
	}
	cmd.Flags().StringVar(&scenario, "scenario", "", "Path to a versioned replay scenario JSON file")
	cmd.Flags().StringVar(&output, "output", "", "New directory for replay result artifacts")
	cmd.Flags().StringVar(&patch, "source-patch", "", "Optional path to the source patch used to build a dirty simulator binary")
	cmd.Flags().DurationVar(&timeout, "timeout", 5*time.Minute, "Maximum host elapsed time")
	return cmd
}
