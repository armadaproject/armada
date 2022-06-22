package cmd

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"

	"github.com/G-Research/armada/internal/testsuite"
	"github.com/G-Research/armada/pkg/client"
)

// RootCmd is the root Cobra command that gets called from the main func.
// All other sub-commands should be registered here.
func RootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "testsuite",
		Short: "testsuite is a suite of automated tests for Armada deployments.",
		Long: `testsuite is a suite of automated tests for Armada deployments.

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
		versionCmd(testsuite.New()),
		testCmd(testsuite.New()),
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
func testCmd(app *testsuite.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "test",
		Short: "Test an Armada deployment by submitting jobs and watching for expected events.",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, app)
		},
		RunE: func(cmd *cobra.Command, args []string) error {

			testFilesPattern, err := cmd.Flags().GetString("tests")
			if err != nil {
				return err
			}

			testFiles, err := filepath.Glob(testFilesPattern)
			if err != nil {
				return err
			}

			for _, testFile := range testFiles {
				start := time.Now()
				err := app.TestFile(testFile)
				fmt.Printf("\nRuntime: %s\n", time.Since(start))
				if err != nil {
					fmt.Printf("TEST FAILED: %s\n", err)
				} else {
					fmt.Print("TEST SUCCEEDED\n")
				}
			}
			return nil
		},
	}

	cmd.Flags().String("tests", "", "Test file pattern, e.g., './testcases/*.yaml'.")

	return cmd
}

func initParams(cmd *cobra.Command, app *testsuite.App) error {
	client.LoadCommandlineArgs()
	app.Params.ApiConnectionDetails = client.ExtractCommandlineArmadaApiConnectionDetails()
	return nil
}
