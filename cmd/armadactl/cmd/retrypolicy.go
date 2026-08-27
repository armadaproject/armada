package cmd

import (
	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/internal/armadactl"
)

func retryPolicyCreateCmd() *cobra.Command {
	a := armadactl.New()
	return retryPolicyFileCmd(a,
		"Create a retry policy from a YAML/JSON file",
		"Create a retry policy that defines rules for whether failed jobs should be retried.",
		a.CreateRetryPolicyFromFile)
}

func retryPolicyUpdateCmd() *cobra.Command {
	a := armadactl.New()
	return retryPolicyFileCmd(a,
		"Update a retry policy from a YAML/JSON file",
		"Update an existing retry policy with the definition from a YAML/JSON file.",
		a.UpdateRetryPolicyFromFile)
}

func retryPolicyGetCmd() *cobra.Command {
	a := armadactl.New()
	return retryPolicyNameCmd(a,
		"Get a retry policy by name",
		"Get the definition of a retry policy by its name.",
		a.GetRetryPolicy)
}

func retryPolicyDeleteCmd() *cobra.Command {
	a := armadactl.New()
	return retryPolicyNameCmd(a,
		"Delete a retry policy by name",
		"Delete an existing retry policy by its name.",
		a.DeleteRetryPolicy)
}

func retryPolicyGetAllCmd() *cobra.Command {
	a := armadactl.New()
	return &cobra.Command{
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
}

// retryPolicyFileCmd builds a command that reads a retry policy from a
// YAML/JSON file and applies it via run.
func retryPolicyFileCmd(a *armadactl.App, short, long string, run func(fileName string) error) *cobra.Command {
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
			return run(filePath)
		},
	}
	cmd.Flags().StringP("file", "f", "", "Path to YAML/JSON file defining the retry policy.")
	if err := cmd.MarkFlagRequired("file"); err != nil {
		panic(err)
	}
	return cmd
}

// retryPolicyNameCmd builds a command that takes a single retry policy name
// argument and applies it via run.
func retryPolicyNameCmd(a *armadactl.App, short, long string, run func(name string) error) *cobra.Command {
	return &cobra.Command{
		Use:   "retry-policy <name>",
		Short: short,
		Long:  long,
		Args:  cobra.ExactArgs(1),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(args[0])
		},
	}
}
