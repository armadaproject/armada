package cmd

import (
	"github.com/armadaproject/armada/internal/armadactl"

	"github.com/spf13/cobra"
)

func createCmd(a *armadactl.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create Armada resource. Supported: queue",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			filePath, _ := cmd.Flags().GetString("file")
			dryRun, _ := cmd.Flags().GetBool("dry-run")

			return a.CreateResource(filePath, dryRun)
		},
	}
	cmd.Flags().StringP("file", "f", "", "specify file for resource creation.")
	if err := cmd.MarkFlagRequired("file"); err != nil {
		panic(err)
	}
	cmd.Flags().Bool("dry-run", false, "Validate the input file and exit without making any changes.")
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
