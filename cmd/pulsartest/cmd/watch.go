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
			flags, err := processCmdFlags(cmd.Flags())
			if err != nil {
				return err
			}

			params := pulsartest.Params{
				Pulsar: configuration.PulsarConfig{
					Enabled:               true,
					URL:                   flags.url,
					AuthenticationEnabled: flags.authEnable,
					AuthenticationType:    flags.authType,
					JwtTokenPath:          flags.jwtPath,
					JobsetEventsTopic:     flags.topic,
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
