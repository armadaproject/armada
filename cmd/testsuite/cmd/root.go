package cmd

import (
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/G-Research/armada/internal/testsuite"
	"github.com/G-Research/armada/pkg/client"
	"github.com/G-Research/armada/pkg/client/domain"
	"github.com/G-Research/armada/pkg/client/util"
	"github.com/G-Research/armada/pkg/client/validation"
)

// RootCmd is the root Cobra command that gets called from the main func.
// All other sub-commands should be registered here.
func RootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "testsuite",
		Short: "testsuite contains a suite of automated tests for Armada clusters.",
		Long: `testsuite contains a suite of automated tests for Armada clusters.


Persistent config can be saved in a config file so it doesn't have to be specified every command.

Example structure:
armadaUrl: localhost:50051
basicAuth:
username: user1
password: password123

The location of this file can be passed in using the --config argument.
If not provided, $HOME/.armadactl.yaml is used.`}

	client.AddArmadaApiConnectionCommandlineArgs(cmd)

	cmd.AddCommand(
		submitCmd(testsuite.New()),
		versionCmd(testsuite.New()),
	)

	return cmd
}

// Print version info and exit.
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

// Submit batches of jobs and wait for those jobs to finish.
// Prints job completion statistics on exit.
func submitCmd(app *testsuite.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "submit",
		Short: "Submit batches of jobs and wait for them to complete.",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, app)
		},
		RunE: func(cmd *cobra.Command, args []string) error {

			jobFilePath, err := cmd.Flags().GetString("jobFile")
			if err != nil {
				return err
			}

			numBatches, err := cmd.Flags().GetUint("numBatches")
			if err != nil {
				return err
			}

			batchSize, err := cmd.Flags().GetUint("batchSize")
			if err != nil {
				return err
			}

			timeout, err := cmd.Flags().GetFloat64("timeout")
			if err != nil {
				return err
			}

			submitInterval, err := cmd.Flags().GetFloat64("submitInterval")
			if err != nil {
				return err
			}

			randomizeJobSet, err := cmd.Flags().GetBool("randomizeJobSet")
			if err != nil {
				return err
			}

			ok, err := validation.ValidateSubmitFile(jobFilePath)
			if !ok {
				return err
			}

			jobFile := &domain.JobSubmitFile{}
			err = util.BindJsonOrYaml(jobFilePath, jobFile)
			if err != nil {
				return err
			}

			if randomizeJobSet {
				jobFile.JobSetId = uuid.New().String()
			}
			return app.Submit(&testsuite.SubmitConfig{
				JobFile:    jobFile,
				NumBatches: numBatches,
				BatchSize:  batchSize,
				Timeout:    time.Duration(int64(timeout*1e9)) * time.Nanosecond,
				Interval:   time.Duration(int64(submitInterval*1e9)) * time.Nanosecond,
			})
		},
	}

	cmd.Flags().String("jobFile", "", "Path to job file to submit.")
	cmd.Flags().Uint("numBatches", 1, "Number of job batches to submit. Submits forever if set to zero.")
	cmd.Flags().Uint("batchSize", 1, "Number of jobs per batch.")
	cmd.Flags().Float64("timeout", 0, "Number of seconds to wait for jobs to finish.")
	cmd.Flags().Float64("submitInterval", 0.01, "Number of seconds between submitting batches of jobs.")
	cmd.Flags().Bool("randomizeJobSet", true, "Whether to replace the job set with a randomly generated string.")

	return cmd
}

func initParams(cmd *cobra.Command, app *testsuite.App) error {
	client.LoadCommandlineArgs()
	app.Params.ApiConnectionDetails = client.ExtractCommandlineArmadaApiConnectionDetails()
	return nil
}
