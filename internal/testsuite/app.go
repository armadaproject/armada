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

	"github.com/G-Research/armada/internal/common/armadaerrors"
	"github.com/G-Research/armada/internal/testsuite/build"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
	"github.com/G-Research/armada/pkg/client/domain"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
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

	ctx, _ := context.WithDeadline(context.Background(), time.Now().Add(config.Timeout))
	g, ctx := errgroup.WithContext(ctx)

	// Ids of submitted jobs.
	// ch := make(chan string)

	// Goroutine responsible for monitoring running jobs.
	// Store the sequence of event types received for each job.
	transitionsById := make(map[string][]string)
	g.Go(func() error {
		return client.WithEventClient(a.Params.ApiConnectionDetails, func(c api.EventClient) error {
			stream, err := c.GetJobSetEvents(ctx, &api.JobSetRequest{
				Id:    config.JobFile.JobSetId,
				Queue: config.JobFile.Queue,
				Watch: true,
			})
			if err != nil {
				return err
			}

			for {
				msg, err := stream.Recv()
				if err == io.EOF {
					return nil
				} else if err != nil {
					return err
				}

				var jobId string
				switch e := msg.Message.Events.(type) {
				case *api.EventMessage_Submitted:
					jobId = e.Submitted.JobId
					transitionsById[jobId] = append(transitionsById[jobId], fmt.Sprintf("%T", e.Submitted))
				case *api.EventMessage_Queued:
					jobId = e.Queued.JobId
					transitionsById[jobId] = append(transitionsById[jobId], fmt.Sprintf("%T", e.Queued))
				case *api.EventMessage_DuplicateFound:
					jobId = e.DuplicateFound.JobId
					transitionsById[jobId] = append(transitionsById[jobId], fmt.Sprintf("%T", e.DuplicateFound))
				case *api.EventMessage_Leased:
					jobId = e.Leased.JobId
					transitionsById[jobId] = append(transitionsById[jobId], fmt.Sprintf("%T", e.Leased))
				case *api.EventMessage_LeaseReturned:
					jobId = e.LeaseReturned.JobId
					transitionsById[jobId] = append(transitionsById[jobId], fmt.Sprintf("%T", e.LeaseReturned))
				case *api.EventMessage_LeaseExpired:
					jobId = e.LeaseExpired.JobId
					transitionsById[jobId] = append(transitionsById[jobId], fmt.Sprintf("%T", e.LeaseExpired))
				case *api.EventMessage_Pending:
					jobId = e.Pending.JobId
					transitionsById[jobId] = append(transitionsById[jobId], fmt.Sprintf("%T", e.Pending))
				case *api.EventMessage_Running:
					jobId = e.Running.JobId
					transitionsById[jobId] = append(transitionsById[jobId], fmt.Sprintf("%T", e.Running))
				case *api.EventMessage_UnableToSchedule:
					jobId = e.UnableToSchedule.JobId
					transitionsById[jobId] = append(transitionsById[jobId], fmt.Sprintf("%T", e.UnableToSchedule))
				case *api.EventMessage_Failed:
					jobId = e.Failed.JobId
					transitionsById[jobId] = append(transitionsById[jobId], fmt.Sprintf("%T", e.Failed))
				case *api.EventMessage_Succeeded:
					jobId = e.Succeeded.JobId
					transitionsById[jobId] = append(transitionsById[jobId], fmt.Sprintf("%T", e.Succeeded))
				case *api.EventMessage_Reprioritized:
					jobId = e.Reprioritized.JobId
					transitionsById[jobId] = append(transitionsById[jobId], fmt.Sprintf("%T", e.Reprioritized))
				case *api.EventMessage_Cancelling:
					jobId = e.Cancelling.JobId
					transitionsById[jobId] = append(transitionsById[jobId], fmt.Sprintf("%T", e.Cancelling))
				case *api.EventMessage_Cancelled:
					jobId = e.Cancelled.JobId
					transitionsById[jobId] = append(transitionsById[jobId], fmt.Sprintf("%T", e.Cancelled))
				case *api.EventMessage_Terminated:
					jobId = e.Terminated.JobId
					transitionsById[jobId] = append(transitionsById[jobId], fmt.Sprintf("%T", e.Terminated))
				case *api.EventMessage_Utilisation:
					jobId = e.Utilisation.JobId
					transitionsById[jobId] = append(transitionsById[jobId], fmt.Sprintf("%T", e.Utilisation))
				case *api.EventMessage_IngressInfo:
					jobId = e.IngressInfo.JobId
					transitionsById[jobId] = append(transitionsById[jobId], fmt.Sprintf("%T", e.IngressInfo))
				case *api.EventMessage_Reprioritizing:
					jobId = e.Reprioritizing.JobId
					transitionsById[jobId] = append(transitionsById[jobId], fmt.Sprintf("%T", e.Reprioritizing))
				case *api.EventMessage_Updated:
					jobId = e.Updated.JobId
					transitionsById[jobId] = append(transitionsById[jobId], fmt.Sprintf("%T", e.Updated))
				}
				fmt.Println(transitionsById[jobId])
			}
		})
	})

	// Goroutine responsible for submitting jobs.
	req := &api.JobSubmitRequest{
		Queue:    config.JobFile.Queue,
		JobSetId: config.JobFile.JobSetId,
	}
	for i := 0; i < int(config.BatchSize); i++ {
		req.JobRequestItems = append(req.JobRequestItems, config.JobFile.Jobs...)
	}
	g.Go(func() error {
		var numBatchesSent uint
		return client.WithSubmitClient(a.Params.ApiConnectionDetails, func(c api.SubmitClient) error {
			ticker := time.NewTicker(config.Interval)
			defer ticker.Stop()
			for config.NumBatches == 0 || numBatchesSent < config.NumBatches {
				select {
				case <-ticker.C:

					log.Infof("submitting %d jobs", len(req.JobRequestItems))
					ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
					// TODO: Record jobs ids?
					_, err := c.SubmitJobs(ctx, req)
					if err != nil {
						return errors.WithStack(errors.WithMessage(err, "error submitting jobs"))
					}

					// // Send the ids so the listener knows which jobs to look for events on.
					// for _, item := range res.JobResponseItems {
					// 	ch <- item.JobId
					// }
					numBatchesSent++
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		})
	})

	if err := g.Wait(); err != nil {
		log.WithError(err).Error("error submitting jobs or receiving events")
	}

	numJobsFromEventSequence := make(map[string]int)
	for _, events := range transitionsById {
		eventSequence := strings.Join(events, " -> ")
		eventSequence = strings.ReplaceAll(eventSequence, "*api.Job", "")
		eventSequence = strings.ReplaceAll(eventSequence, "Event", "")
		numJobsFromEventSequence[eventSequence]++
	}
	for eventSequence, numJobs := range numJobsFromEventSequence {
		fmt.Println(eventSequence, ": ", numJobs)
	}
	return nil
}
