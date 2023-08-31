package submitter

import (
	"sync"
	"time"

	"github.com/armadaproject/armada/internal/common/context"

	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

var submissionSerializer sync.Mutex

type Submitter struct {
	ApiConnectionDetails *client.ApiConnectionDetails
	// Jobs to submit.
	Queue      string
	JobSetName string
	Jobs       []*api.JobSubmitRequestItem
	// If true, generate random clientId for Job, if not already provided
	RandomClientId bool
	// Number of batches of jobs to submit.
	// A value of 0 indicates infinity.
	NumBatches uint32
	// Number of copies of the provided job to submit per batch.
	BatchSize uint32
	// Time between batches.
	Interval time.Duration
	// Number of seconds to wait for jobs to finish.
	Timeout time.Duration
	jobIds  []string
	mu      sync.Mutex
}

func NewSubmitterFromTestSpec(conn *client.ApiConnectionDetails, testSpec *api.TestSpec) *Submitter {
	return &Submitter{
		ApiConnectionDetails: conn,
		Jobs:                 testSpec.Jobs,
		Queue:                testSpec.Queue,
		JobSetName:           testSpec.JobSetId,
		NumBatches:           testSpec.NumBatches,
		BatchSize:            testSpec.BatchSize,
		Interval:             testSpec.Interval,
		RandomClientId:       testSpec.RandomClientId,
	}
}

func (config *Submitter) Validate() error {
	if len(config.Jobs) == 0 {
		return errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "Jobs",
			Value:   config.Jobs,
			Message: "no jobs provided",
		})
	}
	if config.Queue == "" {
		return errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "Queue",
			Value:   config.Queue,
			Message: "not provided",
		})
	}
	if config.JobSetName == "" {
		return errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "JobSetName",
			Value:   config.JobSetName,
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

func (srv *Submitter) Run(ctx *context.ArmadaContext) error {
	var numBatchesSent uint32
	req := &api.JobSubmitRequest{
		Queue:    srv.Queue,
		JobSetId: srv.JobSetName,
	}
	for i := 0; i < int(srv.BatchSize); i++ {
		// workaround to always create new structs instead of copying the pointer
		// that way, we can change some job values without affecting other jobs
		for _, job := range srv.Jobs {
			cloned := *job
			req.JobRequestItems = append(req.JobRequestItems, &cloned)
		}
	}
	if srv.RandomClientId {
		client.AddClientIds(req.JobRequestItems)
	}
	return client.WithSubmitClient(srv.ApiConnectionDetails, func(c api.SubmitClient) error {
		// Create a closed ticker channel; receiving on tickerCh returns immediately.
		C := make(chan time.Time)
		close(C)
		tickerCh := (<-chan time.Time)(C)

		// If an interval is provided, replace tickerCh with one that generates ticks periodically.
		if srv.Interval != 0 {
			ticker := time.NewTicker(srv.Interval)
			defer ticker.Stop()
			tickerCh = ticker.C
		}

		// Submit jobs.
		for srv.NumBatches == 0 || numBatchesSent < srv.NumBatches {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-tickerCh:
				// Stagger submissions by a couple of millis to avoid Kerberos replay issues.
				submissionSerializer.Lock()
				res, err := c.SubmitJobs(ctx, req)
				time.Sleep(5 * time.Millisecond)
				submissionSerializer.Unlock()

				if err != nil {
					return errors.WithMessage(err, "error submitting jobs")
				}
				srv.mu.Lock()
				for _, item := range res.JobResponseItems {
					srv.jobIds = append(srv.jobIds, item.JobId)
				}
				srv.mu.Unlock()
				numBatchesSent++
			}
		}
		return nil
	})
}

func (srv *Submitter) JobIds() []string {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	return srv.jobIds[:]
}
