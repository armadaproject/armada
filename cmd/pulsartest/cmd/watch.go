package cmd

import (
	"github.com/spf13/cobra"

	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/pulsartest"
)

func watchCmd() *cobra.Command {
	a := &pulsartest.App{}

	cmd := &cobra.Command{
		Use:   "watch",
		Short: "Watch for Pulsar events",
		Long:  "Watch for Pulsar events",
		Args:  cobra.ExactArgs(0),
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
			a, err = pulsartest.New(params, "watch")
			return err
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.Watch()
		},
	}

	return cmd
}
