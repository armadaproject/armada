package testsuite

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/G-Research/armada/internal/common/armadaerrors"
	"github.com/G-Research/armada/internal/testsuite/build"
	"github.com/G-Research/armada/internal/testsuite/eventwatcher"
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

	ctx, cancel := context.WithCancel(context.Background())
	if config.Timeout != 0 {
		ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(config.Timeout))
	}
	g, ctx := errgroup.WithContext(ctx)

	// Goroutine for receiving events.
	// Sends events on eventChannel.
	watcher := eventwatcher.New(config.JobFile.Queue, config.JobFile.JobSetId, a.Params.ApiConnectionDetails)
	g.Go(func() error { return watcher.Run(ctx) })

	// eventChannel := make(chan *api.EventMessage)
	// g.Go(func() error {
	// 	return client.WithEventClient(a.Params.ApiConnectionDetails, func(c api.EventClient) error {
	// 		stream, err := c.GetJobSetEvents(ctx, &api.JobSetRequest{
	// 			Id:    config.JobFile.JobSetId,
	// 			Queue: config.JobFile.Queue,
	// 			Watch: true,
	// 		})
	// 		if err != nil {
	// 			return err
	// 		}

	// 		for {
	// 			msg, err := stream.Recv()
	// 			if err == io.EOF {
	// 				return nil
	// 			} else if err != nil {
	// 				return err
	// 			}
	// 			eventChannel <- msg.Message
	// 		}
	// 	})
	// })

	// Goroutine responsible for submitting jobs.
	// Sends the ids of submitted jobs on a channel.
	req := &api.JobSubmitRequest{
		Queue:    config.JobFile.Queue,
		JobSetId: config.JobFile.JobSetId,
	}
	for i := 0; i < int(config.BatchSize); i++ {
		req.JobRequestItems = append(req.JobRequestItems, config.JobFile.Jobs...)
	}
	submitChannel := make(chan string)
	g.Go(func() error {
		var numBatchesSent uint
		return client.WithSubmitClient(a.Params.ApiConnectionDetails, func(c api.SubmitClient) error {
			ticker := time.NewTicker(config.Interval)
			defer ticker.Stop()
			for config.NumBatches == 0 || numBatchesSent < config.NumBatches {
				select {
				case <-ticker.C:
					res, err := c.SubmitJobs(ctx, req)
					if err != nil {
						return errors.WithStack(errors.WithMessage(err, "error submitting jobs"))
					}

					// Send the ids.
					for _, item := range res.JobResponseItems {
						submitChannel <- item.JobId
					}
					numBatchesSent++
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		})
	})

	// Log periodically.
	transitionsByJobId := make(map[string][]string)
	g.Go(func() error {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		defer cancel()

		numTotalJobs := len(req.JobRequestItems)
		numTotalSucceded := 0
		numTotalFailed := 0

		intervalTransitionsByJobId := make(map[string][]string)
		numSubmitted := 0
		numSucceded := 0
		numFailed := 0
		numActive := 0

		for numTotalFailed+numTotalSucceded < numTotalJobs {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C: // Print a summary of what happened in this interval.
				if len(intervalTransitionsByJobId) == 0 {
					break
				}

				numActive += numSubmitted
				numActive -= numSucceded + numFailed

				// For each job for which we already have some state transitions,
				// add the most recent state to the state transitions seen in this interval.
				// This makes is more clear what's going on.
				continuedTransitionsByJobId := make(map[string][]string)
				for jobId, transitions := range intervalTransitionsByJobId {
					if previousTransitions := transitionsByJobId[jobId]; len(previousTransitions) > 0 {
						previousState := previousTransitions[len(previousTransitions)-1]
						continuedTransitionsByJobId[jobId] = append([]string{previousState}, transitions...)
					} else {
						continuedTransitionsByJobId[jobId] = transitions
					}
				}

				// Print the number of jobs for each unique sequence of state transitions.
				for transitions, counts := range countJobsByTransitions(continuedTransitionsByJobId) {
					fmt.Printf("%d:\t%s\n", counts, transitions)
				}
				fmt.Println() // Indicates the end of the interval.
				// fmt.Printf("> %d active jobs (%d submitted, %d succeded, and %d failed in interval)\n\n", numActive, numSubmitted, numSucceded, numFailed)

				// Move transitions over to the global map and reset the interval map.
				for jobId, transitions := range intervalTransitionsByJobId {
					transitionsByJobId[jobId] = append(transitionsByJobId[jobId], transitions...)
				}
				intervalTransitionsByJobId = make(map[string][]string)

				numTotalSucceded += numSucceded
				numTotalFailed += numFailed

				fmt.Println("succeeded", numTotalSucceded, "failed", numTotalFailed)

				numSubmitted = 0
				numSucceded = 0
				numFailed = 0

			case e := <-watcher.C: // Jobset event received.
				if e == nil {
					break
				}

				jobId := jobIdFromApiEvent(e)
				s := shortStringFromApiEvent(e)
				intervalTransitionsByJobId[jobId] = append(intervalTransitionsByJobId[jobId], s)
				if isSubmittedEvent(e) {
					numSubmitted++
				}
				if isSuccededEvent(e) {
					numSucceded++
				}
				if isFailedEvent(e) {
					numFailed++
				}
			case jobId := <-submitChannel:
				intervalTransitionsByJobId[jobId] = append(intervalTransitionsByJobId[jobId], "SubmitSent")
			}
		}
		return nil
	})

	if err := g.Wait(); err != context.DeadlineExceeded && err != context.Canceled && err != nil {
		log.WithError(err).Error("error submitting jobs or receiving events: %s", err)
	}

	fmt.Println("Global count")
	for transitions, counts := range countJobsByTransitions(transitionsByJobId) {
		fmt.Printf("%d:\t%s\n", counts, transitions)
	}
	return nil
}

// countJobsByTransitions returns a map from sequences of transitions,
// e.g., "submitted -> queued" to the number of jobs going through exactly those transitions.
func countJobsByTransitions(transitionsByJobId map[string][]string) map[string]int {
	numJobsFromEventSequence := make(map[string]int)
	for _, events := range transitionsByJobId {
		eventSequence := strings.Join(events, " -> ")
		numJobsFromEventSequence[eventSequence]++
	}
	return numJobsFromEventSequence
}

func shortStringFromApiEvent(msg *api.EventMessage) string {
	s := stringFromApiEvent(msg)
	s = strings.ReplaceAll(s, "*api.EventMessage_", "")
	return s
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

func stringFromApiEvent(msg *api.EventMessage) string {
	return fmt.Sprintf("%T", msg.Events)
}

func jobIdFromApiEvent(msg *api.EventMessage) string {
	switch e := msg.Events.(type) {
	case *api.EventMessage_Submitted:
		return e.Submitted.JobId
	case *api.EventMessage_Queued:
		return e.Queued.JobId
	case *api.EventMessage_DuplicateFound:
		return e.DuplicateFound.JobId
	case *api.EventMessage_Leased:
		return e.Leased.JobId
	case *api.EventMessage_LeaseReturned:
		return e.LeaseReturned.JobId
	case *api.EventMessage_LeaseExpired:
		return e.LeaseExpired.JobId
	case *api.EventMessage_Pending:
		return e.Pending.JobId
	case *api.EventMessage_Running:
		return e.Running.JobId
	case *api.EventMessage_UnableToSchedule:
		return e.UnableToSchedule.JobId
	case *api.EventMessage_Failed:
		return e.Failed.JobId
	case *api.EventMessage_Succeeded:
		return e.Succeeded.JobId
	case *api.EventMessage_Reprioritized:
		return e.Reprioritized.JobId
	case *api.EventMessage_Cancelling:
		return e.Cancelling.JobId
	case *api.EventMessage_Cancelled:
		return e.Cancelled.JobId
	case *api.EventMessage_Terminated:
		return e.Terminated.JobId
	case *api.EventMessage_Utilisation:
		return e.Utilisation.JobId
	case *api.EventMessage_IngressInfo:
		return e.IngressInfo.JobId
	case *api.EventMessage_Reprioritizing:
		return e.Reprioritizing.JobId
	case *api.EventMessage_Updated:
		return e.Updated.JobId
	}
	return ""
}
