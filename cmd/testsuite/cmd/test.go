package cmd

import (
	"context"
	"encoding/xml"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/jstemmer/go-junit-report/v2/junit"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	"github.com/G-Research/armada/internal/testsuite"
)

// Submit batches of jobs and wait for those jobs to finish.
// Prints job completion statistics on exit.
func testCmd(app *testsuite.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "test",
		Short: "Test an Armada deployment by submitting jobs and watching for expected events.",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, app)
		},
		RunE: testCmdRunE(app),
	}

	cmd.Flags().String("tests", "", "Test file pattern, e.g., './testcases/*.yaml'.")
	cmd.Flags().String("junit", "", "Write a JUnit test report to this path.")

	return cmd
}

func testCmdRunE(app *testsuite.App) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		if err := app.Params.ApiConnectionDetails.ArmadaRestServerHealthcheck(); err != nil {
			return errors.Wrap(err, "armada server health check failed")
		}

		testFilesPattern, err := cmd.Flags().GetString("tests")
		if err != nil {
			return errors.WithStack(err)
		}

		testFiles, err := filepath.Glob(testFilesPattern)
		if err != nil {
			return errors.WithStack(err)
		}

		junitPath, err := cmd.Flags().GetString("junit")
		if err != nil {
			return errors.WithStack(err)
		}

		// Create a context that is cancelled on SIGINT/SIGTERM.
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

		testSuite := &junit.Testsuite{
			Name: testFilesPattern,
		}

		numSuccesses, numFailures := runTestFiles(ctx, app, testSuite, testFiles)

		start := time.Now()

		printSummary(start, numSuccesses, numFailures)

		// If junitPath is set, write a JUnit report.
		testSuite.Time = fmt.Sprint(time.Since(start))
		testSuites := &junit.Testsuites{
			Name: testFilesPattern,
			Time: testSuite.Time,
		}
		testSuites.AddSuite(*testSuite)
		if junitPath != "" {
			if err := createJUnitReport(junitPath, testSuites); err != nil {
				return errors.Wrap(err, "error creating junit report")
			}
		}

		if numFailures != 0 {
			return errors.Errorf("there was at least one test failure")
		}
		return nil
	}
}

func runTestFiles(ctx context.Context, app *testsuite.App, testSuite *junit.Testsuite, testFiles []string) (numSuccesses, numFailures int) {
	for _, testFile := range testFiles {
		testStart := time.Now()
		testCase, err := app.TestFileJunit(ctx, testFile)
		fmt.Printf("\nRuntime: %s\n", time.Since(testStart))
		if err != nil {
			numFailures++
			fmt.Printf("TEST FAILED: %s\n", err)
		} else {
			numSuccesses++
			fmt.Printf("TEST SUCCEEDED\n")
		}

		testSuite.AddTestcase(testCase)
	}
	return numSuccesses, numFailures
}

func printSummary(start time.Time, numSuccesses, numFailures int) {
	fmt.Printf("\n======= SUMMARY =======\n")
	fmt.Printf("Ran %d test(s) in %s\n", numSuccesses+numFailures, time.Since(start))
	fmt.Printf("Successes: %d\n", numSuccesses)
	fmt.Printf("Failures: %d\n", numFailures)
}

func createJUnitReport(junitPath string, testSuites *junit.Testsuites) error {
	junitFile, err := os.Create(junitPath)
	if err != nil {
		return errors.WithStack(err)
	}
	defer junitFile.Close()

	encoder := xml.NewEncoder(junitFile)
	encoder.Indent("", "\t")

	if err = encoder.Encode(testSuites); err != nil {
		return errors.WithStack(err)
	}
	if err = encoder.Flush(); err != nil {
		return errors.WithStack(err)
	}

	return nil
}
