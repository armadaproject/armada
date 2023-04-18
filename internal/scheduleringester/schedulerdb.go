package scheduleringester

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/common/ingest"
	"github.com/armadaproject/armada/internal/common/ingest/metrics"
	schedulerdb "github.com/armadaproject/armada/internal/scheduler/database"
)

// SchedulerDb writes DbOperations into postgres.
type SchedulerDb struct {
	// Connection to the postgres database.
	db             *pgxpool.Pool
	metrics        *metrics.Metrics
	initialBackOff time.Duration
	maxBackOff     time.Duration
}

func NewSchedulerDb(db *pgxpool.Pool, metrics *metrics.Metrics, initialBackOff time.Duration, maxBackOff time.Duration) ingest.Sink[*DbOperationsWithMessageIds] {
	return &SchedulerDb{db: db, metrics: metrics, initialBackOff: initialBackOff, maxBackOff: maxBackOff}
}

func (s *SchedulerDb) Store(ctx context.Context, instructions *DbOperationsWithMessageIds) error {
	var result *multierror.Error = nil
	for _, dbOp := range instructions.Ops {
		err := ingest.WithRetry(func() (bool, error) {
			err := s.WriteDbOp(ctx, dbOp)
			shouldRetry := armadaerrors.IsNetworkError(err) || armadaerrors.IsRetryablePostgresError(err)
			return shouldRetry, err
		}, s.initialBackOff, s.maxBackOff)
		result = multierror.Append(result, err)
	}
	return result.ErrorOrNil()
}

func (s *SchedulerDb) WriteDbOp(ctx context.Context, op DbOperation) error {
	queries := schedulerdb.New(s.db)
	switch o := op.(type) {
	case InsertJobs:
		records := make([]any, len(o))
		i := 0
		for _, v := range o {
			records[i] = *v
			i++
		}
		err := database.Upsert(ctx, s.db, "jobs", records)
		if err != nil {
			return err
		}
	case InsertRuns:
		records := make([]any, len(o))
		i := 0
		for _, v := range o {
			records[i] = *v
			i++
		}
		err := database.Upsert(ctx, s.db, "runs", records)
		if err != nil {
			return err
		}
	case UpdateJobSetPriorities:
		for jobSet, priority := range o {
			err := queries.UpdateJobPriorityByJobSet(
				ctx,
				schedulerdb.UpdateJobPriorityByJobSetParams{
					JobSet:   jobSet,
					Priority: priority,
				},
			)
			if err != nil {
				return errors.WithStack(err)
			}
		}
	case UpdateJobSchedulingInfo:
		args := make([]interface{}, 0, len(o)*3)
		argMarkers := make([]string, 0, len(o))

		currentIndex := 1
		for key, value := range o {
			args = append(args, key)
			args = append(args, value.JobSchedulingInfo)
			args = append(args, value.JobSchedulingInfoVersion)
			argMarkers = append(argMarkers, fmt.Sprintf("($%d, $%d::bytea, $%d::int)", currentIndex, currentIndex+1, currentIndex+2))
			currentIndex += 3
		}

		argMarkersString := strings.Join(argMarkers, ",")
		updateJobInfoSqlStatement := fmt.Sprintf(
			`update jobs as j set  scheduling_info = updated.scheduling_info, scheduling_info_version = updated.scheduling_info_version
				 from (values %s) as updated(job_id, scheduling_info, scheduling_info_version)
                 where j.job_id = updated.job_id and updated.scheduling_info_version > j.scheduling_info_version`, argMarkersString)

		_, err := s.db.Exec(ctx, updateJobInfoSqlStatement, args...)
		if err != nil {
			return errors.WithStack(err)
		}
	case UpdateJobQueuedState:
		args := make([]interface{}, 0, len(o)*3)
		argMarkers := make([]string, 0, len(o))

		currentIndex := 1
		for key, value := range o {
			args = append(args, key)
			args = append(args, value.Queued)
			args = append(args, value.QueuedStateVersion)
			argMarkers = append(argMarkers, fmt.Sprintf("($%d, $%d::bool, $%d::int)", currentIndex, currentIndex+1, currentIndex+2))
			currentIndex += 3
		}

		argMarkersString := strings.Join(argMarkers, ",")
		updateQueuedStateSqlStatement := fmt.Sprintf(
			`update jobs as j set  queued = updated.queued, queued_version = updated.queued_version
				 from (values %s) as updated(job_id, queued, queued_version)
				 where j.job_id = updated.job_id and updated.queued_version > j.queued_version`, argMarkersString)

		_, err := s.db.Exec(ctx, updateQueuedStateSqlStatement, args...)
		if err != nil {
			return errors.WithStack(err)
		}
	case MarkJobSetsCancelRequested:
		jobSets := maps.Keys(o)
		err := queries.MarkJobsCancelRequestedBySets(ctx, jobSets)
		if err != nil {
			return errors.WithStack(err)
		}
	case MarkJobsCancelRequested:
		jobIds := maps.Keys(o)
		err := queries.MarkJobsCancelRequestedById(ctx, jobIds)
		if err != nil {
			return errors.WithStack(err)
		}
	case MarkJobsCancelled:
		jobIds := maps.Keys(o)
		err := queries.MarkJobsCancelledById(ctx, jobIds)
		if err != nil {
			return errors.WithStack(err)
		}
	case MarkJobsSucceeded:
		jobIds := maps.Keys(o)
		err := queries.MarkJobsSucceededById(ctx, jobIds)
		if err != nil {
			return errors.WithStack(err)
		}
	case MarkJobsFailed:
		jobIds := maps.Keys(o)
		err := queries.MarkJobsFailedById(ctx, jobIds)
		if err != nil {
			return errors.WithStack(err)
		}
	case UpdateJobPriorities:
		// TODO: This will be slow if there's a large number of ids.
		// Could be addressed by using a separate table for priority + upsert.
		for jobId, priority := range o {
			err := queries.UpdateJobPriorityById(ctx, schedulerdb.UpdateJobPriorityByIdParams{
				JobID:    jobId,
				Priority: priority,
			})
			if err != nil {
				return errors.WithStack(err)
			}
		}
	case MarkRunsSucceeded:
		runIds := maps.Keys(o)
		err := queries.MarkJobRunsSucceededById(ctx, runIds)
		if err != nil {
			return errors.WithStack(err)
		}
	case MarkRunsFailed:
		runIds := maps.Keys(o)
		returned := make([]uuid.UUID, 0, len(runIds))
		runAttempted := make([]uuid.UUID, 0, len(runIds))
		for k, v := range o {
			if v.LeaseReturned {
				returned = append(returned, k)
			}
			if v.RunAttempted {
				runAttempted = append(runAttempted, k)
			}
		}
		err := queries.MarkJobRunsFailedById(ctx, runIds)
		if err != nil {
			return errors.WithStack(err)
		}
		err = queries.MarkJobRunsReturnedById(ctx, returned)
		if err != nil {
			return errors.WithStack(err)
		}
		err = queries.MarkJobRunsAttemptedById(ctx, runAttempted)
		if err != nil {
			return errors.WithStack(err)
		}
	case MarkRunsRunning:
		runIds := maps.Keys(o)
		err := queries.MarkJobRunsRunningById(ctx, runIds)
		if err != nil {
			return errors.WithStack(err)
		}
	case InsertJobRunErrors:
		records := make([]any, len(o))
		i := 0
		for _, v := range o {
			records[i] = *v
			i++
		}
		return database.Upsert(ctx, s.db, "job_run_errors", records)
	case *InsertPartitionMarker:
		for _, marker := range o.markers {
			err := queries.InsertMarker(ctx, schedulerdb.InsertMarkerParams{
				GroupID:     marker.GroupID,
				PartitionID: marker.PartitionID,
				Created:     marker.Created,
			})
			if err != nil {
				return errors.Wrapf(err, "error inserting partition marker")
			}
		}
		return nil
	default:
		return errors.Errorf("received unexpected op %+v", op)
	}
	return nil
}
