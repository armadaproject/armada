package cmd

import (
	"github.com/G-Research/armada/internal/armadactl"

	"github.com/spf13/cobra"
)

func createCmd(a *armadactl.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create Armada resource. Supported: queue",
	}
	cmd.AddCommand(queueCreateCmd())
	cmd.PreRunE = func(cmd *cobra.Command, args []string) error {
		return initParams(cmd, a.Params)
	}
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		filePath, _ := cmd.Flags().GetString("file")
		dryRun, _ := cmd.Flags().GetBool("dry-run")

		return a.CreateResource(filePath, dryRun)
	}
	cmd.Flags().StringP("file", "f", "", "specify file for resource creation.")
	cmd.MarkFlagRequired("file")
	cmd.Flags().Bool("dry-run", false, "Performs file validation. Does no actual creates the resource.")
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
