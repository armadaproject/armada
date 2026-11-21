package cmd

import (
	"strings"

	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/internal/armadactl"
)

// withOutputFlag adds an optional --output/-o flag to a command
func withOutputFlag(cmd *cobra.Command, a *armadactl.App) *cobra.Command {
	cmd.Flags().StringP("output", "o", "",
		strings.Join(armadactl.OutputFormats, " or ")+". Defaults to the command's usual output.")
	if err := cmd.RegisterFlagCompletionFunc("output", func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
		return armadactl.OutputFormats, cobra.ShellCompDirectiveNoFileComp
	}); err != nil {
		panic(err)
	}

	prev := cmd.PreRunE
	cmd.PreRunE = func(cmd *cobra.Command, args []string) error {
		output, err := cmd.Flags().GetString("output")
		if err != nil {
			return err
		}
		a.Params.Output, err = armadactl.ParseOutputFormat(output)
		if err != nil {
			return err
		}
		if prev != nil {
			return prev(cmd, args)
		}
		return nil
	}
	return cmd
}
