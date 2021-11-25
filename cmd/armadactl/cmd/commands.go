package cmd

import (
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(
		createCmd(),
		deleteCmd(),
		updateCmd(),
		describeCmd(),
	)
}

func createCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create Armada resource. Supported: queue",
	}
	cmd.AddCommand(queueCreateCmd())
	return cmd
}

func deleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete Armada resource. Supported: queue",
	}
	cmd.AddCommand(queueDeleteCmd())
	return cmd
}

func updateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update",
		Short: "Update Armada resource. Supported: queue",
	}
	cmd.AddCommand(queueUpdateCmd())
	return cmd
}

func describeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "describe",
		Short: "Retrieve information about armada resource. Supported: queue",
	}
	cmd.AddCommand(queueDescribeCmd())
	return cmd
}
