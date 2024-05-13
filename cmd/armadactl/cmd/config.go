package cmd

import (
	"fmt"
	"strings"

	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/exp/slices"

	"github.com/armadaproject/armada/internal/armadactl"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/pkg/client"
)

func configCmd(a *armadactl.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Operations on armadactl config. Supported: use-context, get-contexts, current-context",
	}

	cmd.AddCommand(getContextsCmd(a))
	cmd.AddCommand(useContextCmd(a))
	cmd.AddCommand(currentContextCmd(a))
	return cmd
}

func useContextCmd(a *armadactl.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "use-context <context>",
		Short: "Sets the default context for future armadactl calls",
		Long: `This command allows you to set the default context value for future armadactl calls.

It stores this state within the specified, or otherwise default ($HOME/.armadactl) configuration file.`,
		Args: cobra.ExactArgs(1),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			contextToUse := args[0]
			if !slices.Contains(client.ExtractConfigurationContexts(), contextToUse) {
				return fmt.Errorf("unable to use context %s. Context not defined in config.", contextToUse)
			}

			cfg, err := cmd.Flags().GetString("config")
			if err != nil {
				return err
			} else if cfg != "" {
				return client.ModifyCurrentContextInConfig(cfg, contextToUse)
			}

			homeDir, err := homedir.Dir()
			if err != nil {
				return fmt.Errorf("error getting home directory: %s", err)
			}

			err = client.ModifyCurrentContextInConfig(fmt.Sprintf("%s/.armadactl.yaml", homeDir), contextToUse)
			if err == nil {
				fmt.Printf("Current context set to %s\n", contextToUse)
			}

			return err
		},
	}

	return cmd
}

func getContextsCmd(a *armadactl.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get-contexts",
		Short: "Retrieves all contexts from armadactl config",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			currentContext := viper.GetString("currentContext")
			allContexts := armadaslices.Map(client.ExtractConfigurationContexts(), func(context string) string {
				if context == currentContext {
					return fmt.Sprintf("%s (current)", context)
				}
				return context
			})

			slices.Sort(allContexts)

			fmt.Printf("Available contexts: %s\n", strings.Join(allContexts, ", "))
			return nil
		},
	}
	return cmd
}

func currentContextCmd(a *armadactl.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "current-context",
		Short: "Returns the currently set Armada context, determined from armadactl config",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Printf("Current context: %s\n", viper.GetString("currentContext"))
			return nil
		},
	}
	return cmd
}
