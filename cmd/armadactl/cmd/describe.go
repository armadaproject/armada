package cmd

import (
	"github.com/spf13/cobra"
)

func describeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "describe",
		Short: "Describe Armada resources",
	}

	cmd.AddCommand(describeNodeCmd())
	cmd.AddCommand(describePodCmd())

	return cmd
}
