package cmd

import (
	"github.com/spf13/cobra"

	"github.com/G-Research/armada/internal/armadactl"
)

func init() {
	rootCmd.AddCommand(versionCmd())
}

func versionCmd() *cobra.Command {
	a := armadactl.New()
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Print client version information",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.Version()
		},
	}
	return cmd
}
