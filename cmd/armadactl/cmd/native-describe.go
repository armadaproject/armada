package cmd 

import (
	"github.com/spf13/cobra"
)

func nativeDescribeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use: "native-describe",
		Short: "Describe Armada resources with native Armada data",
	}

	cmd.AddCommand(nativeDescribeNodeCmd())
	cmd.AddCommand(nativeDescribePodCmd())

	return cmd
}