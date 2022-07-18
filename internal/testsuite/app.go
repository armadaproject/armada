package testsuite

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/hashicorp/go-multierror"
	"github.com/jstemmer/go-junit-report/v2/junit"
	"github.com/pkg/errors"
	"github.com/renstrom/shortuuid"
	"golang.org/x/sync/errgroup"
	apimachineryYaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"

	"github.com/G-Research/armada/internal/testsuite/build"
	"github.com/G-Research/armada/internal/testsuite/eventlogger"
	"github.com/G-Research/armada/internal/testsuite/eventsplitter"
	"github.com/G-Research/armada/internal/testsuite/eventwatcher"
	"github.com/G-Research/armada/internal/testsuite/submitter"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
)

type App struct {
	// Parameters passed to the CLI by the user.
	Params *Params
	// Out is used to write the output. Defaults to standard out,
	// but can be overridden in tests to make assertions on the applications's output.
	Out io.Writer
	// Source of randomness. Tests can use a mocked random source in order to provide
	// deterministic testing behavior.
	Random io.Reader
}

// Params struct holds all user-customizable parameters.
// Using a single struct for all CLI commands ensures that all flags are distinct
// and that they can be provided either dynamically on a command line, or
// statically in a config file that's reused between command runs.
type Params struct {
	ApiConnectionDetails *client.ApiConnectionDetails
}

// New instantiates an App with default parameters, including standard output
// and cryptographically secure random source.
func New() *App {
	return &App{
		Params: &Params{},
		Out:    os.Stdout,
		Random: rand.Reader,
	}
}

// validateParams validates a.Params. Currently, it doesn't check anything.
func (a *App) validateParams() error {
	return nil
}

// Version prints build information (e.g., current git commit) to the app output.
func (a *App) Version() error {
	w := tabwriter.NewWriter(a.Out, 1, 1, 1, ' ', 0)
	defer w.Flush()
	fmt.Fprintf(w, "Version:\t%s\n", build.ReleaseVersion)
	fmt.Fprintf(w, "Commit:\t%s\n", build.GitCommit)
	fmt.Fprintf(w, "Go version:\t%s\n", build.GoVersion)
	fmt.Fprintf(w, "Built:\t%s\n", build.BuildTime)
	return nil
}

func (a *App) TestFileJunit(ctx context.Context, filePath string) (junit.Testcase, error) {

	// Load test spec.
	testSpec, err := TestSpecFromFilePath(filePath)
	if err != nil {
		return junit.Testcase{}, err
	}

	// Capture the test output for inclusion in the returned junit.Testcase.
	// Before returning, restore the original a.Out.
	origOut := a.Out
	var out bytes.Buffer
	a.Out = io.MultiWriter(&out, origOut)
	defer func() { a.Out = origOut }()

	// Run the tests and convert any errors into a Junit result.
	start := time.Now()
	testErr := a.Test(ctx, testSpec)
	var failure *junit.Result
	if testErr != nil {
		failure = &junit.Result{
			Message: fmt.Sprintf("%+v", testErr),
		}
	}
	systemOut := &junit.Output{
		Data: out.String(),
	}

	// Return Junit TestCase according to spec: https://llg.cubic.org/docs/junit/
	return junit.Testcase{
		Name:      testSpec.Name,
		Classname: filePath,
		Time:      fmt.Sprint(time.Since(start)),
		Failure:   failure,
		SystemOut: systemOut,
	}, testErr
}

func (a *App) TestFile(ctx context.Context, filePath string) error {
	testSpec, err := TestSpecFromFilePath(filePath)
	if err != nil {
		return err
	}
	return a.Test(ctx, testSpec)
}

func TestSpecFromFilePath(filePath string) (*api.TestSpec, error) {
	testSpec := &api.TestSpec{}
	yamlBytes, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if err := UnmarshalTestCase(yamlBytes, testSpec); err != nil {
		return nil, err
	}

	// Randomize jobSetName for each test to ensure we're only getting events for this run.
	fileName := filepath.Base(filePath)
	fileName = strings.TrimSuffix(fileName, filepath.Ext(fileName))
	testSpec.JobSetId = fileName + "-" + shortuuid.New()

	// If no test name is provided, set it to be the filename.
	if testSpec.Name == "" {
		testSpec.Name = fileName
	}

	return testSpec, nil
}

// GetCancelAllJobs returns a processor that cancels all jobs in jobIds one at a time
// and then consumes events until ctx is cancelled.
func GetCancelAllJobs(testSpec *api.TestSpec, apiConnectionDetails *client.ApiConnectionDetails) func(context.Context, chan *api.EventMessage, map[string]bool) error {
	return func(ctx context.Context, ch chan *api.EventMessage, jobIds map[string]bool) error {
		return client.WithSubmitClient(apiConnectionDetails, func(sc api.SubmitClient) error {
			for jobId := range jobIds {
				_, err := sc.CancelJobs(ctx, &api.JobCancelRequest{
					JobId:    jobId,
					Queue:    testSpec.GetQueue(),
					JobSetId: testSpec.GetJobSetId(),
				})
				if err != nil {
					return err
				}
			}
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-ch:
				}
			}
		})
	}
}

func (a *App) Test(ctx context.Context, testSpec *api.TestSpec, asserters ...func(context.Context, chan *api.EventMessage, map[string]bool) error) error {

	logInterval := 5 * time.Second
	fmt.Fprintf(a.Out, "\n======= %s =======\n", testSpec.GetName())
	fmt.Fprintf(a.Out, "Queue: %s\n", testSpec.GetQueue())
	fmt.Fprintf(a.Out, "Job set: %s\n", testSpec.GetJobSetId())
	fmt.Fprintf(a.Out, "Timeout: %s\n", testSpec.GetTimeout())
	fmt.Fprintf(a.Out, "Log interval: %s\n", logInterval)
	fmt.Fprint(a.Out, "\n")
	fmt.Fprintf(a.Out, "Expected events:\n")
	for _, e := range testSpec.GetExpectedEvents() {
		s := fmt.Sprintf("%T", e.GetEvents())
		s = strings.ReplaceAll(s, "*api.EventMessage_", "")
		fmt.Fprintf(a.Out, "- %s\n", s)
	}
	fmt.Fprint(a.Out, "\n")
	fmt.Fprintf(a.Out, "Job transitions over windows of length %s:\n", logInterval)

	// Optional timeout
	ctx, cancel := context.WithCancel(ctx)
	if testSpec.Timeout != 0 {
		ctx, cancel = context.WithTimeout(ctx, testSpec.Timeout)
	}
	defer cancel()

	// Submit jobs.
	submitter := &submitter.Submitter{
		ApiConnectionDetails: a.Params.ApiConnectionDetails,
		Jobs:                 testSpec.Jobs,
		Queue:                testSpec.Queue,
		JobSetName:           testSpec.JobSetId,
		NumBatches:           testSpec.NumBatches,
		BatchSize:            testSpec.BatchSize,
		Interval:             testSpec.Interval,
	}
	err := submitter.Run(ctx)
	if err != nil {
		return err
	}
	jobIds := submitter.JobIds()
	jobIdMap := make(map[string]bool)
	for _, jobId := range jobIds {
		jobIdMap[jobId] = false
	}

	// If configured, cancel the submitted jobs.
	if testSpec.Cancel == api.TestSpec_BY_ID {
		err := client.WithSubmitClient(a.Params.ApiConnectionDetails, func(sc api.SubmitClient) error {
			for _, jobId := range jobIds {
				_, err := sc.CancelJobs(ctx, &api.JobCancelRequest{
					JobId:    jobId,
					Queue:    testSpec.GetQueue(),
					JobSetId: testSpec.GetJobSetId(),
				})
				if err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	} else if testSpec.Cancel == api.TestSpec_BY_SET {
		err := client.WithSubmitClient(a.Params.ApiConnectionDetails, func(sc api.SubmitClient) error {
			_, err := sc.CancelJobs(ctx, &api.JobCancelRequest{
				Queue:    testSpec.GetQueue(),
				JobSetId: testSpec.GetJobSetId(),
			})
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	// One channel for each system listening to events.
	logCh := make(chan *api.EventMessage)
	noActiveCh := make(chan *api.EventMessage)
	assertCh := make(chan *api.EventMessage)
	ingressCh := make(chan *api.EventMessage)

	// Setup an errgroup that cancels on any job failing or there being no active jobs.
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return eventwatcher.ErrorOnNoActiveJobs(ctx, noActiveCh, jobIdMap) })

	// Logger service.
	eventLogger := eventlogger.New(logCh, logInterval)
	eventLogger.Out = a.Out
	g.Go(func() error { return eventLogger.Run(ctx) })

	// Goroutine forwarding API events on a channel.
	watcher := eventwatcher.New(testSpec.Queue, testSpec.JobSetId, a.Params.ApiConnectionDetails)
	watcher.BackoffExponential = time.Second
	watcher.MaxRetries = 6
	watcher.Out = a.Out
	g.Go(func() error { return watcher.Run(ctx) })

	// Split the events into multiple channels, one for each downstream service.
	splitter := eventsplitter.New(watcher.C, []chan *api.EventMessage{assertCh, ingressCh, logCh, noActiveCh}...)
	g.Go(func() error { return splitter.Run(ctx) })

	// Watch for ingress events and try to download from any ingresses found.
	g.Go(func() error { return eventwatcher.GetFromIngresses(ctx, ingressCh) })

	// Assert that we get the right events for each job.
	err = eventwatcher.AssertEvents(ctx, assertCh, jobIdMap, testSpec.ExpectedEvents)

	// Stop all services and wait for them to exit.
	cancel()
	groupErr := g.Wait()
	if err != nil && groupErr != nil && groupErr != context.Canceled {
		err = errors.WithMessage(groupErr, err.Error())
	}

	fmt.Fprint(a.Out, "All job transitions:\n")
	eventLogger.Log()

	// Cancel any jobs we haven't seen a terminal event for.
	client.WithSubmitClient(a.Params.ApiConnectionDetails, func(sc api.SubmitClient) error {
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cancelGroup, ctx := errgroup.WithContext(ctx)
		for jobId, hasTerminated := range jobIdMap {
			if hasTerminated {
				continue
			}
			jobId := jobId
			cancelGroup.Go(func() error {
				res, err := sc.CancelJobs(ctx, &api.JobCancelRequest{
					Queue:    testSpec.GetQueue(),
					JobSetId: testSpec.GetJobSetId(),
					JobId:    jobId,
				})
				if err != nil {
					fmt.Fprintf(a.Out, "\nError cancelling jobs: %s\n", err.Error())
				} else if len(res.GetCancelledIds()) > 0 {
					fmt.Fprintf(a.Out, "\nCancelled %v\n", res.GetCancelledIds())
				}
				return err
			})
		}
		return cancelGroup.Wait()
	})

	return err
}

// UnmarshalTestCase unmarshals bytes into a TestSpec.
func UnmarshalTestCase(yamlBytes []byte, testSpec *api.TestSpec) error {
	var result *multierror.Error
	successExpectedEvents := false
	successEverythingElse := false
	docs := bytes.Split(yamlBytes, []byte("---"))
	for _, docYamlBytes := range docs {

		// yaml.Unmarshal can unmarshal everything,
		// but leaves oneof fields empty (these are silently discarded).
		if err := apimachineryYaml.NewYAMLOrJSONDecoder(bytes.NewReader(yamlBytes), 128).Decode(testSpec); err != nil {
			result = multierror.Append(result, err)
		} else {
			successEverythingElse = true
		}

		// YAMLToJSON + jsonpb.Unmarshaler can unmarshal oneof fields,
		// but can't unmarshal k8s pod specs.
		docJsonBytes, err := yaml.YAMLToJSON(docYamlBytes)
		if err != nil {
			result = multierror.Append(result, err)
			continue
		}
		unmarshaler := jsonpb.Unmarshaler{AllowUnknownFields: true}
		err = unmarshaler.Unmarshal(bytes.NewReader(docJsonBytes), testSpec)
		if err != nil {
			result = multierror.Append(result, err)
		} else {
			successExpectedEvents = true
		}
	}
	if !successExpectedEvents || !successEverythingElse {
		return result.ErrorOrNil()
	}
	return nil
}
