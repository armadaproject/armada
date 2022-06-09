package testsuite

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"text/tabwriter"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

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

func (a *App) Submit(config *SubmitConfig) error {
	if err := config.Validate(); err != nil {
		return err
	}

	// Optional
	// ctx := context.Background()
	ctx, cancel := context.WithCancel(context.Background())
	if config.Timeout != 0 {
		ctx, cancel = context.WithTimeout(ctx, config.Timeout)
	}

	// Channels for each downstream system.
	logCh := make(chan *api.EventMessage)
	noActiveCh := make(chan *api.EventMessage)
	failedCh := make(chan *api.EventMessage)
	assertCh := make(chan *api.EventMessage)

	// Setup ctx to cancel on any job failing and on no active jobs.
	ctx = eventwatcher.WithCancelOnFailed(ctx, failedCh)
	ctx = eventwatcher.WithCancelOnNoActiveJobs(ctx, noActiveCh)

	g, ctx := errgroup.WithContext(ctx)

	// Goroutine for receiving events.
	watcher := eventwatcher.New(config.JobFile.Queue, config.JobFile.JobSetId, a.Params.ApiConnectionDetails)
	g.Go(func() error { return watcher.Run(ctx) })

	// Split the events into multiple channels, one for each downstream service.
	// noActiveCh and failedCh must come last, since they cancel the context.
	splitter := eventsplitter.New(watcher.C, []chan *api.EventMessage{assertCh, logCh, noActiveCh, failedCh}...)
	g.Go(func() error { return splitter.Run(ctx) })

	// Log periodically.
	eventLogger := eventlogger.New(logCh, 5*time.Second)
	g.Go(func() error { return eventLogger.Run(ctx) })

	// Submit jobs
	submitter := &submitter.Submitter{
		ApiConnectionDetails: a.Params.ApiConnectionDetails,
		JobFile:              config.JobFile,
		NumBatches:           config.NumBatches,
		BatchSize:            config.BatchSize,
		Interval:             config.Interval,
	}
	err := submitter.Run(ctx)
	if err != nil {
		return err
	}

	jobIds := submitter.JobIds()
	expected := []*api.EventMessage{
		{Events: &api.EventMessage_Submitted{}},
		{Events: &api.EventMessage_Succeeded{}},
	}
	jobIdMap := make(map[string]interface{})
	for _, jobId := range jobIds {
		jobIdMap[jobId] = ""
	}

	err = eventwatcher.AssertEvents(ctx, assertCh, jobIdMap, expected)
	if err != nil {
		return err
	}
	cancel()

	// // Goroutine responsible for submitting jobs.
	// // Sends the ids of submitted jobs on a channel.
	// req := &api.JobSubmitRequest{
	// 	Queue:    config.JobFile.Queue,
	// 	JobSetId: config.JobFile.JobSetId,
	// }
	// for i := 0; i < int(config.BatchSize); i++ {
	// 	req.JobRequestItems = append(req.JobRequestItems, config.JobFile.Jobs...)
	// }
	// g.Go(func() error {
	// 	fmt.Println("Submitter started")
	// 	defer fmt.Println("Submitter stopped")
	// 	var numBatchesSent uint
	// 	return client.WithSubmitClient(a.Params.ApiConnectionDetails, func(c api.SubmitClient) error {
	// 		ticker := time.NewTicker(config.Interval)
	// 		defer ticker.Stop()
	// 		for config.NumBatches == 0 || numBatchesSent < config.NumBatches {
	// 			select {
	// 			case <-ticker.C:
	// 				_, err := c.SubmitJobs(ctx, req)
	// 				if err != nil {
	// 					return errors.WithStack(errors.WithMessage(err, "error submitting jobs"))
	// 				}
	// 				numBatchesSent++
	// 			case <-ctx.Done():
	// 				return ctx.Err()
	// 			}
	// 		}
	// 		return nil
	// 	})
	// })

	// Wait for ctx to be cancelled or time out.
	// <-ctx.Done()

	fmt.Println("Waiting for errgroup")
	if err := g.Wait(); err != context.DeadlineExceeded && err != context.Canceled && err != nil {
		log.WithError(err).Error("error submitting jobs or receiving events: %s", err)
	}
	fmt.Println("Errgroup cancelled")

	fmt.Println("All transitions")
	eventLogger.Log()

	return nil
}

func isSubmittedEvent(msg *api.EventMessage) bool {
	switch msg.Events.(type) {
	case *api.EventMessage_Submitted:
		return true
	}
	return false
}

func isSuccededEvent(msg *api.EventMessage) bool {
	switch msg.Events.(type) {
	case *api.EventMessage_Succeeded:
		return true
	}
	return false
}

func isFailedEvent(msg *api.EventMessage) bool {
	switch msg.Events.(type) {
	case *api.EventMessage_Failed:
		return true
	}
	return false
}

func isTerminalEvent(msg *api.EventMessage) bool {
	switch msg.Events.(type) {
	case *api.EventMessage_Failed:
		return true
	case *api.EventMessage_Succeeded:
		return true
	}
	return false
}
