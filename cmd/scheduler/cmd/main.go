package cmd

import (
	"github.com/armadaproject/armada/internal/scheduler"
	"github.com/spf13/cobra"
)

func runCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Runs the scheduler",
		RunE:  runScheduler,
	}
	return cmd
}

func runScheduler(_ *cobra.Command, _ []string) error {
	config, err := loadConfig()
	if err != nil {
		return err
	}
	return scheduler.Run(config)
}
