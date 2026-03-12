package cmd

import (
	"github.com/spf13/cobra"
)

func kubeDescribeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use: "kube-describe",
		Short: "Describe Armada resources with cached kubectl data",
	}

	cmd.AddCommand(kubeDescribeNodeCmd())
	cmd.AddCommand(kubeDescribePodCmd())

	return cmd
}