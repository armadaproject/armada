package submitter

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/G-Research/armada/internal/common/armadaerrors"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
	"github.com/G-Research/armada/pkg/client/domain"
	"github.com/pkg/errors"
)

type Submitter struct {
	ApiConnectionDetails *client.ApiConnectionDetails
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
	jobIds  []string
	mu      sync.Mutex
	C       chan string
}

func (config *Submitter) Validate() error {
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

func (srv *Submitter) Run(ctx context.Context) error {
	fmt.Println("Submitter started")
	defer fmt.Println("Submitter stopped")

	srv.C = make(chan string)
	defer close(srv.C)

	var numBatchesSent uint
	req := &api.JobSubmitRequest{
		Queue:    srv.JobFile.Queue,
		JobSetId: srv.JobFile.JobSetId,
	}
	for i := 0; i < int(srv.BatchSize); i++ {
		req.JobRequestItems = append(req.JobRequestItems, srv.JobFile.Jobs...)
	}
	return client.WithSubmitClient(srv.ApiConnectionDetails, func(c api.SubmitClient) error {
		ticker := time.NewTicker(srv.Interval)
		defer ticker.Stop()
		for srv.NumBatches == 0 || numBatchesSent < srv.NumBatches {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				res, err := c.SubmitJobs(ctx, req)
				if err != nil {
					return errors.WithStack(errors.WithMessage(err, "error submitting jobs"))
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
