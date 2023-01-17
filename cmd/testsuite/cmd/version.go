package cmd

import (
	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/internal/testsuite"
)

// versionCmd prints version info and exits.
func versionCmd(app *testsuite.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Print version.",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, app)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return app.Version()
		},
	}
	return cmd
}
