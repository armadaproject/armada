package cmd

import (
	"context"
	"encoding/xml"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/jstemmer/go-junit-report/v2/junit"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/internal/testsuite"
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
	cmd.Flags().String("benchmark", "", "Write a benchmark test report to this path.")
	cmd.Flags().String("prometheusPushgatewayUrl", "", "Push metrics to Prometheus pushgateway at this url.")
	cmd.Flags().String("prometheusPushgatewayJobName", "armada-testsuite", "Metrics are annotated with with job=prometheusPushGatewayJobName.")
	return cmd
}

func testCmdRunE(app *testsuite.App) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		if app.Params.ApiConnectionDetails.ArmadaRestUrl != "" {
			healthy, err := app.Params.ApiConnectionDetails.ArmadaHealthCheck()
			if err != nil {
				return errors.WithMessage(err, "failed to perform health check")
			}
			if !healthy {
				return errors.New("health check failed")
			}
		}

		testFilesPattern, err := cmd.Flags().GetString("tests")
		if err != nil {
			return errors.WithStack(err)
		}

		junitPath, err := cmd.Flags().GetString("junit")
		if err != nil {
			return errors.WithStack(err)
		}

		benchmarkPath, err := cmd.Flags().GetString("benchmark")
		if err != nil {
			return errors.WithStack(err)
		}
		if benchmarkPath != "" {
			return errors.New("benchmark report not currently supported")
		}

		prometheusPushgatewayUrl, err := cmd.Flags().GetString("prometheusPushgatewayUrl")
		if err != nil {
			return errors.WithStack(err)
		}
		app.Params.PrometheusPushGatewayUrl = prometheusPushgatewayUrl

		prometheusPushgatewayJobName, err := cmd.Flags().GetString("prometheusPushgatewayJobName")
		if err != nil {
			return errors.WithStack(err)
		}
		app.Params.PrometheusPushGatewayJobName = prometheusPushgatewayJobName

		// Create a context that is cancelled on SIGINT/SIGTERM.
		// Ensures test jobs are cancelled on ctrl-c.
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

		start := time.Now()
		testSuiteReport, err := app.TestPattern(ctx, testFilesPattern)
		if err != nil {
			return err
		}
		junitTestSuite := &junit.Testsuite{
			Name: testFilesPattern,
		}
		for _, testCaseReport := range testSuiteReport.TestCaseReports {
			junitTestSuite.AddTestcase(testCaseReport.JunitTestCase())
		}

		numSuccesses := testSuiteReport.NumSuccesses()
		numFailures := testSuiteReport.NumFailures()
		fmt.Printf("\n======= SUMMARY =======\n")
		w := tabwriter.NewWriter(os.Stdout, 1, 1, 1, ' ', 0)
		fmt.Fprint(w, "Test:\tResult:\tElapsed:\n")
		for _, testCaseReport := range testSuiteReport.TestCaseReports {
			var result string
			if testCaseReport.FailureReason == "" {
				result = "SUCCESS"
			} else {
				result = "FAILURE"
			}
			elapsed := testCaseReport.Finish.Sub(testCaseReport.Start)
			fmt.Fprintf(w, "%s\t%s\t%s\n", testCaseReport.TestSpec.Name, result, elapsed)
		}
		_ = w.Flush()
		fmt.Println()
		fmt.Printf("Ran %d test(s) in %s\n", numSuccesses+numFailures, time.Since(start))
		fmt.Printf("Success: %d\n", numSuccesses)
		fmt.Printf("Failure: %d\n", numFailures)
		fmt.Println()

		// If junitPath is set, write a JUnit report.
		junitTestSuite.Time = fmt.Sprint(time.Since(start))
		junitTestSuites := &junit.Testsuites{
			Name: testFilesPattern,
			Time: junitTestSuite.Time,
		}
		junitTestSuites.AddSuite(*junitTestSuite)
		if junitPath != "" {
			if err := writeJUnitReport(junitPath, junitTestSuites); err != nil {
				return errors.WithMessage(err, "error writing junit report")
			}
		}

		if numFailures != 0 {
			return errors.Errorf("test failure")
		}
		return nil
	}
}

func writeJUnitReport(junitPath string, testSuites *junit.Testsuites) error {
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
