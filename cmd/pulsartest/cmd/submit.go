package cmd

import (
	"github.com/spf13/cobra"

	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/pulsartest"
)

func submitCmd() *cobra.Command {
	a := &pulsartest.App{}

	cmd := &cobra.Command{
		Use:   "submit ./path/to/events.yaml",
		Short: "Submit events to Pulsar",
		Long: `Submit events to Pulsar from file.
`,
		Args: cobra.ExactArgs(1),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			url, err := cmd.Flags().GetString("url")
			if err != nil {
				return err
			}
			authEnabled, err := cmd.Flags().GetBool("authenticationEnabled")
			if err != nil {
				return err
			}
			authType, err := cmd.Flags().GetString("authenticationType")
			if err != nil {
				return err
			}
			jwtPath, err := cmd.Flags().GetString("jwtTokenPath")
			if err != nil {
				return err
			}
			jsTopic, err := cmd.Flags().GetString("jobsetEventsTopic")
			if err != nil {
				return err
			}

			params := pulsartest.Params{
				Pulsar: configuration.PulsarConfig{
					Enabled:               true,
					URL:                   url,
					AuthenticationEnabled: authEnabled,
					AuthenticationType:    authType,
					JwtTokenPath:          jwtPath,
					JobsetEventsTopic:     jsTopic,
				},
			}
			a, err = pulsartest.New(params)
			return err
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			path := args[0]
			return a.Submit(path, false)
		},
	}

	return cmd
}
