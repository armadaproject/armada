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
		Long:  "Submit events to Pulsar from file.",
		Args:  cobra.ExactArgs(1),
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
			a, err = pulsartest.New(params, "submit")
			return err
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			path := args[0]
			return a.Submit(path)
		},
	}

	return cmd
}
