package submitter

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/G-Research/armada/internal/common/armadaerrors"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
)

type Submitter struct {
	ApiConnectionDetails *client.ApiConnectionDetails
	// Jobs to submit.
	Queue      string
	JobSetName string
	Jobs       []*api.JobSubmitRequestItem
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

func (srv *Submitter) Run(ctx context.Context) error {
	var numBatchesSent uint32
	req := &api.JobSubmitRequest{
		Queue:    srv.Queue,
		JobSetId: srv.JobSetName,
	}
	for i := 0; i < int(srv.BatchSize); i++ {
		req.JobRequestItems = append(req.JobRequestItems, srv.Jobs...)
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
				res, err := c.SubmitJobs(ctx, req)
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
