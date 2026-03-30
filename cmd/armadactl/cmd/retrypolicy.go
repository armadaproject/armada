package cmd

import (
	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/internal/armadactl"
)

func retryPolicyCreateCmd() *cobra.Command {
	return retryPolicyCreateCmdWithApp(armadactl.New())
}

func retryPolicyCreateCmdWithApp(a *armadactl.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "retry-policy",
		Short: "Create a retry policy from a YAML/JSON file",
		Long:  "Create a retry policy that defines rules for whether failed jobs should be retried.",
		Args:  cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			filePath, err := cmd.Flags().GetString("file")
			if err != nil {
				return err
			}
			return a.CreateRetryPolicyFromFile(filePath)
		},
	}
	cmd.Flags().StringP("file", "f", "", "Path to YAML/JSON file defining the retry policy.")
	if err := cmd.MarkFlagRequired("file"); err != nil {
		panic(err)
	}
	return cmd
}

func retryPolicyUpdateCmd() *cobra.Command {
	return retryPolicyUpdateCmdWithApp(armadactl.New())
}

func retryPolicyUpdateCmdWithApp(a *armadactl.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "retry-policy",
		Short: "Update a retry policy from a YAML/JSON file",
		Long:  "Update an existing retry policy with the definition from a YAML/JSON file.",
		Args:  cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			filePath, err := cmd.Flags().GetString("file")
			if err != nil {
				return err
			}
			return a.UpdateRetryPolicyFromFile(filePath)
		},
	}
	cmd.Flags().StringP("file", "f", "", "Path to YAML/JSON file defining the retry policy.")
	if err := cmd.MarkFlagRequired("file"); err != nil {
		panic(err)
	}
	return cmd
}

func retryPolicyGetCmd() *cobra.Command {
	return retryPolicyGetCmdWithApp(armadactl.New())
}

func retryPolicyGetCmdWithApp(a *armadactl.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "retry-policy <name>",
		Short: "Get a retry policy by name",
		Long:  "Get the definition of a retry policy by its name.",
		Args:  cobra.ExactArgs(1),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.GetRetryPolicy(args[0])
		},
	}
	return cmd
}

func retryPolicyGetAllCmd() *cobra.Command {
	return retryPolicyGetAllCmdWithApp(armadactl.New())
}

func retryPolicyGetAllCmdWithApp(a *armadactl.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "retry-policies",
		Short: "List all retry policies",
		Long:  "List all retry policies defined in the system.",
		Args:  cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.GetAllRetryPolicies()
		},
	}
	return cmd
}

func retryPolicyDeleteCmd() *cobra.Command {
	return retryPolicyDeleteCmdWithApp(armadactl.New())
}

func retryPolicyDeleteCmdWithApp(a *armadactl.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "retry-policy <name>",
		Short: "Delete a retry policy by name",
		Long:  "Delete an existing retry policy by its name.",
		Args:  cobra.ExactArgs(1),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.DeleteRetryPolicy(args[0])
		},
	}
	return cmd
}
