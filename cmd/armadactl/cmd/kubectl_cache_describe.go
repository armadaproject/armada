package cmd

import (
	"github.com/spf13/cobra"
)

func kubectlCacheDescribeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use: "kubectl-cache-describe",
		Short: "Describe Armada resources with cached kubectl data",
	}

	cmd.AddCommand(kubectlCacheDescribeNodeCmd())
	cmd.AddCommand(kubectlCacheDescribePodCmd())

	return cmd
}