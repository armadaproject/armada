package cmd

import (
	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/internal/armadactl"
)

func jobGetCmd() *cobra.Command {
	a := armadactl.New()
	return withOutputFlag(&cobra.Command{
		Use:   "job <job-id>",
		Short: "Get information about a job",
		Long: "Get the full details of a job, given by its id: its state, resources and runs, and the job spec. " +
			"The job is read from Lookout, so the lookoutUrl of the context (or --lookoutUrl) must be set. " +
			"The details are printed as yaml, or as json with --output json.",
		Args: cobra.ExactArgs(1),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.GetJob(args[0])
		},
	}, a)
}
