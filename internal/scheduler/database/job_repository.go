package database

import (
	"context"

	"github.com/google/uuid"

	"github.com/G-Research/armada/pkg/armadaevents"
)

// JobRepository is an interface to be implemented by structs which provide job and job run information
type JobRepository interface {

	// FetchJobUpdates returns all jobs and job runs that have been updated after jobSerial and jobRunSerial respectively
	// These updates are guaranteed to be consistent with each other
	FetchJobUpdates(ctx context.Context, jobSerial int64, jobRunSerial int64) ([]Job, []Run, error)

	// FetchJobRunErrors returns all armadaevents.JobRunErrors for the provided job run ids.  The returned map is
	// keyed by job run id.  Any runs which  don't have errros wil be absent from the map.
	FetchJobRunErrors(ctx context.Context, runIds []uuid.UUID) (map[uuid.UUID]*armadaevents.JobRunErrors, error)

	// CountReceivedPartitions returns a count of the number of partition messages present in the database corresponding
	// to the provided groupId.  This is used by  the scheduler to determine if the database represents the state of
	// pulsar after a given point in time.
	CountReceivedPartitions(ctx context.Context, groupId uuid.UUID) (uint32, error)
}
