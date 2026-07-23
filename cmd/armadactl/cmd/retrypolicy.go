package cmd

import (
	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/internal/armadactl"
)

func retryPolicyCreateCmd() *cobra.Command {
	return retryPolicyCreateCmdWithApp(armadactl.New())
}

func retryPolicyCreateCmdWithApp(a *armadactl.App) *cobra.Command {
	return retryPolicyFileCmdWithApp(a,
		"Create a retry policy from a YAML/JSON file",
		"Create a retry policy that defines rules for whether failed jobs should be retried.",
		(*armadactl.App).CreateRetryPolicyFromFile)
}

func retryPolicyUpdateCmd() *cobra.Command {
	return retryPolicyUpdateCmdWithApp(armadactl.New())
}

func retryPolicyUpdateCmdWithApp(a *armadactl.App) *cobra.Command {
	return retryPolicyFileCmdWithApp(a,
		"Update a retry policy from a YAML/JSON file",
		"Update an existing retry policy with the definition from a YAML/JSON file.",
		(*armadactl.App).UpdateRetryPolicyFromFile)
}

// retryPolicyFileCmdWithApp builds a command that reads a retry policy from a
// YAML/JSON file and applies it via run.
func retryPolicyFileCmdWithApp(a *armadactl.App, short, long string, run func(*armadactl.App, string) error) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "retry-policy",
		Short: short,
		Long:  long,
		Args:  cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			filePath, err := cmd.Flags().GetString("file")
			if err != nil {
				return err
			}
			return run(a, filePath)
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
	return retryPolicyNameCmdWithApp(a,
		"Get a retry policy by name",
		"Get the definition of a retry policy by its name.",
		(*armadactl.App).GetRetryPolicy)
}

// retryPolicyNameCmdWithApp builds a command that takes a single retry policy
// name argument and applies it via run.
func retryPolicyNameCmdWithApp(a *armadactl.App, short, long string, run func(*armadactl.App, string) error) *cobra.Command {
	return &cobra.Command{
		Use:   "retry-policy <name>",
		Short: short,
		Long:  long,
		Args:  cobra.ExactArgs(1),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(a, args[0])
		},
	}
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
	return retryPolicyNameCmdWithApp(a,
		"Delete a retry policy by name",
		"Delete an existing retry policy by its name.",
		(*armadactl.App).DeleteRetryPolicy)
}
