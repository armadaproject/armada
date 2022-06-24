package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
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

			// Crate a context that is cancelled on SIGINT/SIGTERM.
			// Ensures test jobs are cancelled on ctrl-C.
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			stopSignal := make(chan os.Signal, 1)
			signal.Notify(stopSignal, syscall.SIGINT, syscall.SIGTERM)
			go func() {
				select {
				case <-ctx.Done():
					return
				case <-stopSignal:
					cancel()
				}
			}()

			numSuccesses := 0
			numFailures := 0
			start := time.Now()
			for _, testFile := range testFiles {
				testStart := time.Now()
				err := app.TestFile(ctx, testFile)
				fmt.Printf("\nRuntime: %s\n", time.Since(testStart))
				if err != nil {
					numFailures++
					fmt.Printf("TEST FAILED: %s\n", err)
				} else {
					numSuccesses++
					fmt.Print("TEST SUCCEEDED\n")
				}
			}

			fmt.Printf("\n======= SUMMARY =======\n")
			fmt.Printf("Ran %d test(s) in %s\n", numSuccesses+numFailures, time.Since(start))
			fmt.Printf("Successes: %d\n", numSuccesses)
			fmt.Printf("Failures: %d\n", numFailures)
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
