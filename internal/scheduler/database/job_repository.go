package database

import (
	"context"

	"github.com/google/uuid"

	"github.com/G-Research/armada/pkg/armadaevents"
)

type JobRepository interface {
	FetchJobUpdates(ctx context.Context, jobSerial int64, jobRunSerial int64) ([]Job, []Run, error)
	FetchJobRunErrors(ctx context.Context, runIds []uuid.UUID) (map[uuid.UUID]*armadaevents.JobRunErrors, error)
	CountReceivedPartitions(ctx context.Context, groupId uuid.UUID) (uint32, error)
}
