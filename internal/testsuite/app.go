package testsuite

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"text/tabwriter"
	"time"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"sigs.k8s.io/yaml"

	"github.com/G-Research/armada/internal/common/armadaerrors"
	"github.com/G-Research/armada/internal/testsuite/build"
	"github.com/G-Research/armada/internal/testsuite/eventlogger"
	"github.com/G-Research/armada/internal/testsuite/eventsplitter"
	"github.com/G-Research/armada/internal/testsuite/eventwatcher"
	"github.com/G-Research/armada/internal/testsuite/submitter"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
	"github.com/G-Research/armada/pkg/client/domain"
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

type TestSpec struct {
	// Jobs to submit.
	// The n jobs herein are copied BatchSize times to produce n*BatchSize jobs.
	// A batch of n*BatchSize such jobs are submitted in each API call.
	// NumBatches such batches are submitted in total.
	JobFile *domain.JobSubmitFile
	// Queue string
	// JobSetId string
	// Job []*api.JobSubmitRequestItem
	// Number of batches of jobs to submit.
	// If 0, will submit forever.
	NumBatches uint
	// Number of copies of the provided jobs to submit per batch.
	BatchSize uint
	// Time between batches.
	// If 0, jobs are submitted as quickly as possible.
	Interval time.Duration
	// Number of seconds to wait for jobs to finish.
	Timeout time.Duration
	// ExpectedEvents []*api.EventMessage
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

type SubmitConfig struct {
	// Jobs to submit.
	JobFile *domain.JobSubmitFile
	// Number of batches of jobs to submit.
	// A value of 0 indicates infinity.
	NumBatches uint
	// Number of copies of the provided job to submit per batch.
	BatchSize uint
	// Time between batches.
	Interval time.Duration
	// Number of seconds to wait for jobs to finish.
	Timeout time.Duration
}

func (config *SubmitConfig) Validate() error {
	if config.JobFile == nil {
		return errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "Job",
			Value:   config.JobFile,
			Message: "not provided",
		})
	}
	if len(config.JobFile.Jobs) == 0 {
		return errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "Jobs",
			Value:   config.JobFile.Jobs,
			Message: "no jobs provided",
		})
	}
	if config.JobFile.Queue == "" {
		return errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "Queue",
			Value:   config.JobFile.Queue,
			Message: "not provided",
		})
	}
	if config.JobFile.JobSetId == "" {
		return errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "JobSetName",
			Value:   config.JobFile.JobSetId,
			Message: "not provided",
		})
	}
	if config.BatchSize <= 0 {
		return errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "BatchSize",
			Value:   config.BatchSize,
			Message: "batch size must be positive",
		})
	}
	return nil
}

func (a *App) Submit(testSpec *api.TestSpec) error {
	// if err := testSpec.Validate(); err != nil {
	// 	return err
	// }

	// Optional timeout
	ctx, cancel := context.WithCancel(context.Background())
	if testSpec.Timeout != 0 {
		fmt.Println("Non-zero timeout ", testSpec.Timeout)
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

	// One channel for each system listening to events.
	logCh := make(chan *api.EventMessage)
	noActiveCh := make(chan *api.EventMessage)
	failedCh := make(chan *api.EventMessage)
	assertCh := make(chan *api.EventMessage)

	// Setup an errgroup that cancels on any job failing or there being no active jobs.
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return eventwatcher.ErrorOnNoActiveJobs(ctx, noActiveCh, jobIdMap) })
	g.Go(func() error { return eventwatcher.ErrorOnFailed(ctx, failedCh) })

	// Logger service.
	eventLogger := eventlogger.New(logCh, 5*time.Second)
	g.Go(func() error { return eventLogger.Run(ctx) })

	// Goroutine forwarding API events on a channel.
	watcher := eventwatcher.New(testSpec.Queue, testSpec.JobSetId, a.Params.ApiConnectionDetails)
	g.Go(func() error { return watcher.Run(ctx) })

	// Split the events into multiple channels, one for each downstream service.
	// noActiveCh and failedCh must come last, since they cancel the context.
	splitter := eventsplitter.New(watcher.C, []chan *api.EventMessage{assertCh, logCh, noActiveCh, failedCh}...)
	g.Go(func() error { return splitter.Run(ctx) })

	// Assert that we get the right events for each job.
	err = eventwatcher.AssertEvents(ctx, assertCh, jobIdMap, testSpec.ExpectedEvents)
	if err != nil {
		fmt.Println("======= assert failed ", err)
		return err
	}
	cancel()

	fmt.Println("Waiting for errgroup")
	if err := g.Wait(); err != context.DeadlineExceeded && err != context.Canceled && err != nil {
		log.WithError(err).Errorf("error submitting jobs or receiving events: %s", err)
	}
	fmt.Println("Errgroup cancelled")

	fmt.Println("All transitions")
	eventLogger.Log()

	return nil
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
		if err := yaml.Unmarshal(docYamlBytes, testSpec); err != nil {
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
