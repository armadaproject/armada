// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.16.0

package database

import (
	"context"
	"database/sql"
	"fmt"
)

type DBTX interface {
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
	PrepareContext(context.Context, string) (*sql.Stmt, error)
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...interface{}) *sql.Row
}

func New(db DBTX) *Queries {
	return &Queries{db: db}
}

func Prepare(ctx context.Context, db DBTX) (*Queries, error) {
	q := Queries{db: db}
	var err error
	if q.countGroupStmt, err = db.PrepareContext(ctx, countGroup); err != nil {
		return nil, fmt.Errorf("error preparing query CountGroup: %w", err)
	}
	if q.deleteOldMarkersStmt, err = db.PrepareContext(ctx, deleteOldMarkers); err != nil {
		return nil, fmt.Errorf("error preparing query DeleteOldMarkers: %w", err)
	}
	if q.findActiveRunsStmt, err = db.PrepareContext(ctx, findActiveRuns); err != nil {
		return nil, fmt.Errorf("error preparing query FindActiveRuns: %w", err)
	}
	if q.insertMarkerStmt, err = db.PrepareContext(ctx, insertMarker); err != nil {
		return nil, fmt.Errorf("error preparing query InsertMarker: %w", err)
	}
	if q.markJobRunsAttemptedByIdStmt, err = db.PrepareContext(ctx, markJobRunsAttemptedById); err != nil {
		return nil, fmt.Errorf("error preparing query MarkJobRunsAttemptedById: %w", err)
	}
	if q.markJobRunsFailedByIdStmt, err = db.PrepareContext(ctx, markJobRunsFailedById); err != nil {
		return nil, fmt.Errorf("error preparing query MarkJobRunsFailedById: %w", err)
	}
	if q.markJobRunsReturnedByIdStmt, err = db.PrepareContext(ctx, markJobRunsReturnedById); err != nil {
		return nil, fmt.Errorf("error preparing query MarkJobRunsReturnedById: %w", err)
	}
	if q.markJobRunsRunningByIdStmt, err = db.PrepareContext(ctx, markJobRunsRunningById); err != nil {
		return nil, fmt.Errorf("error preparing query MarkJobRunsRunningById: %w", err)
	}
	if q.markJobRunsSucceededByIdStmt, err = db.PrepareContext(ctx, markJobRunsSucceededById); err != nil {
		return nil, fmt.Errorf("error preparing query MarkJobRunsSucceededById: %w", err)
	}
	if q.markJobsCancelRequestedByIdStmt, err = db.PrepareContext(ctx, markJobsCancelRequestedById); err != nil {
		return nil, fmt.Errorf("error preparing query MarkJobsCancelRequestedById: %w", err)
	}
	if q.markJobsCancelRequestedBySetAndQueuedStateStmt, err = db.PrepareContext(ctx, markJobsCancelRequestedBySetAndQueuedState); err != nil {
		return nil, fmt.Errorf("error preparing query MarkJobsCancelRequestedBySetAndQueuedState: %w", err)
	}
	if q.markJobsCancelledByIdStmt, err = db.PrepareContext(ctx, markJobsCancelledById); err != nil {
		return nil, fmt.Errorf("error preparing query MarkJobsCancelledById: %w", err)
	}
	if q.markJobsFailedByIdStmt, err = db.PrepareContext(ctx, markJobsFailedById); err != nil {
		return nil, fmt.Errorf("error preparing query MarkJobsFailedById: %w", err)
	}
	if q.markJobsSucceededByIdStmt, err = db.PrepareContext(ctx, markJobsSucceededById); err != nil {
		return nil, fmt.Errorf("error preparing query MarkJobsSucceededById: %w", err)
	}
	if q.markRunsCancelledByJobIdStmt, err = db.PrepareContext(ctx, markRunsCancelledByJobId); err != nil {
		return nil, fmt.Errorf("error preparing query MarkRunsCancelledByJobId: %w", err)
	}
	if q.selectAllExecutorsStmt, err = db.PrepareContext(ctx, selectAllExecutors); err != nil {
		return nil, fmt.Errorf("error preparing query SelectAllExecutors: %w", err)
	}
	if q.selectAllJobIdsStmt, err = db.PrepareContext(ctx, selectAllJobIds); err != nil {
		return nil, fmt.Errorf("error preparing query SelectAllJobIds: %w", err)
	}
	if q.selectAllMarkersStmt, err = db.PrepareContext(ctx, selectAllMarkers); err != nil {
		return nil, fmt.Errorf("error preparing query SelectAllMarkers: %w", err)
	}
	if q.selectAllRunErrorsStmt, err = db.PrepareContext(ctx, selectAllRunErrors); err != nil {
		return nil, fmt.Errorf("error preparing query SelectAllRunErrors: %w", err)
	}
	if q.selectAllRunIdsStmt, err = db.PrepareContext(ctx, selectAllRunIds); err != nil {
		return nil, fmt.Errorf("error preparing query SelectAllRunIds: %w", err)
	}
	if q.selectExecutorUpdateTimesStmt, err = db.PrepareContext(ctx, selectExecutorUpdateTimes); err != nil {
		return nil, fmt.Errorf("error preparing query SelectExecutorUpdateTimes: %w", err)
	}
	if q.selectJobsForExecutorStmt, err = db.PrepareContext(ctx, selectJobsForExecutor); err != nil {
		return nil, fmt.Errorf("error preparing query SelectJobsForExecutor: %w", err)
	}
	if q.selectNewJobsStmt, err = db.PrepareContext(ctx, selectNewJobs); err != nil {
		return nil, fmt.Errorf("error preparing query SelectNewJobs: %w", err)
	}
	if q.selectNewRunsStmt, err = db.PrepareContext(ctx, selectNewRuns); err != nil {
		return nil, fmt.Errorf("error preparing query SelectNewRuns: %w", err)
	}
	if q.selectNewRunsForJobsStmt, err = db.PrepareContext(ctx, selectNewRunsForJobs); err != nil {
		return nil, fmt.Errorf("error preparing query SelectNewRunsForJobs: %w", err)
	}
	if q.selectRunErrorsByIdStmt, err = db.PrepareContext(ctx, selectRunErrorsById); err != nil {
		return nil, fmt.Errorf("error preparing query SelectRunErrorsById: %w", err)
	}
	if q.selectUpdatedJobsStmt, err = db.PrepareContext(ctx, selectUpdatedJobs); err != nil {
		return nil, fmt.Errorf("error preparing query SelectUpdatedJobs: %w", err)
	}
	if q.updateJobPriorityByIdStmt, err = db.PrepareContext(ctx, updateJobPriorityById); err != nil {
		return nil, fmt.Errorf("error preparing query UpdateJobPriorityById: %w", err)
	}
	if q.updateJobPriorityByJobSetStmt, err = db.PrepareContext(ctx, updateJobPriorityByJobSet); err != nil {
		return nil, fmt.Errorf("error preparing query UpdateJobPriorityByJobSet: %w", err)
	}
	if q.upsertExecutorStmt, err = db.PrepareContext(ctx, upsertExecutor); err != nil {
		return nil, fmt.Errorf("error preparing query UpsertExecutor: %w", err)
	}
	return &q, nil
}

func (q *Queries) Close() error {
	var err error
	if q.countGroupStmt != nil {
		if cerr := q.countGroupStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing countGroupStmt: %w", cerr)
		}
	}
	if q.deleteOldMarkersStmt != nil {
		if cerr := q.deleteOldMarkersStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing deleteOldMarkersStmt: %w", cerr)
		}
	}
	if q.findActiveRunsStmt != nil {
		if cerr := q.findActiveRunsStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing findActiveRunsStmt: %w", cerr)
		}
	}
	if q.insertMarkerStmt != nil {
		if cerr := q.insertMarkerStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing insertMarkerStmt: %w", cerr)
		}
	}
	if q.markJobRunsAttemptedByIdStmt != nil {
		if cerr := q.markJobRunsAttemptedByIdStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing markJobRunsAttemptedByIdStmt: %w", cerr)
		}
	}
	if q.markJobRunsFailedByIdStmt != nil {
		if cerr := q.markJobRunsFailedByIdStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing markJobRunsFailedByIdStmt: %w", cerr)
		}
	}
	if q.markJobRunsReturnedByIdStmt != nil {
		if cerr := q.markJobRunsReturnedByIdStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing markJobRunsReturnedByIdStmt: %w", cerr)
		}
	}
	if q.markJobRunsRunningByIdStmt != nil {
		if cerr := q.markJobRunsRunningByIdStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing markJobRunsRunningByIdStmt: %w", cerr)
		}
	}
	if q.markJobRunsSucceededByIdStmt != nil {
		if cerr := q.markJobRunsSucceededByIdStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing markJobRunsSucceededByIdStmt: %w", cerr)
		}
	}
	if q.markJobsCancelRequestedByIdStmt != nil {
		if cerr := q.markJobsCancelRequestedByIdStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing markJobsCancelRequestedByIdStmt: %w", cerr)
		}
	}
	if q.markJobsCancelRequestedBySetAndQueuedStateStmt != nil {
		if cerr := q.markJobsCancelRequestedBySetAndQueuedStateStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing markJobsCancelRequestedBySetAndQueuedStateStmt: %w", cerr)
		}
	}
	if q.markJobsCancelledByIdStmt != nil {
		if cerr := q.markJobsCancelledByIdStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing markJobsCancelledByIdStmt: %w", cerr)
		}
	}
	if q.markJobsFailedByIdStmt != nil {
		if cerr := q.markJobsFailedByIdStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing markJobsFailedByIdStmt: %w", cerr)
		}
	}
	if q.markJobsSucceededByIdStmt != nil {
		if cerr := q.markJobsSucceededByIdStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing markJobsSucceededByIdStmt: %w", cerr)
		}
	}
	if q.markRunsCancelledByJobIdStmt != nil {
		if cerr := q.markRunsCancelledByJobIdStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing markRunsCancelledByJobIdStmt: %w", cerr)
		}
	}
	if q.selectAllExecutorsStmt != nil {
		if cerr := q.selectAllExecutorsStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing selectAllExecutorsStmt: %w", cerr)
		}
	}
	if q.selectAllJobIdsStmt != nil {
		if cerr := q.selectAllJobIdsStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing selectAllJobIdsStmt: %w", cerr)
		}
	}
	if q.selectAllMarkersStmt != nil {
		if cerr := q.selectAllMarkersStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing selectAllMarkersStmt: %w", cerr)
		}
	}
	if q.selectAllRunErrorsStmt != nil {
		if cerr := q.selectAllRunErrorsStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing selectAllRunErrorsStmt: %w", cerr)
		}
	}
	if q.selectAllRunIdsStmt != nil {
		if cerr := q.selectAllRunIdsStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing selectAllRunIdsStmt: %w", cerr)
		}
	}
	if q.selectExecutorUpdateTimesStmt != nil {
		if cerr := q.selectExecutorUpdateTimesStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing selectExecutorUpdateTimesStmt: %w", cerr)
		}
	}
	if q.selectJobsForExecutorStmt != nil {
		if cerr := q.selectJobsForExecutorStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing selectJobsForExecutorStmt: %w", cerr)
		}
	}
	if q.selectNewJobsStmt != nil {
		if cerr := q.selectNewJobsStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing selectNewJobsStmt: %w", cerr)
		}
	}
	if q.selectNewRunsStmt != nil {
		if cerr := q.selectNewRunsStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing selectNewRunsStmt: %w", cerr)
		}
	}
	if q.selectNewRunsForJobsStmt != nil {
		if cerr := q.selectNewRunsForJobsStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing selectNewRunsForJobsStmt: %w", cerr)
		}
	}
	if q.selectRunErrorsByIdStmt != nil {
		if cerr := q.selectRunErrorsByIdStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing selectRunErrorsByIdStmt: %w", cerr)
		}
	}
	if q.selectUpdatedJobsStmt != nil {
		if cerr := q.selectUpdatedJobsStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing selectUpdatedJobsStmt: %w", cerr)
		}
	}
	if q.updateJobPriorityByIdStmt != nil {
		if cerr := q.updateJobPriorityByIdStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing updateJobPriorityByIdStmt: %w", cerr)
		}
	}
	if q.updateJobPriorityByJobSetStmt != nil {
		if cerr := q.updateJobPriorityByJobSetStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing updateJobPriorityByJobSetStmt: %w", cerr)
		}
	}
	if q.upsertExecutorStmt != nil {
		if cerr := q.upsertExecutorStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing upsertExecutorStmt: %w", cerr)
		}
	}
	return err
}

func (q *Queries) exec(ctx context.Context, stmt *sql.Stmt, query string, args ...interface{}) (sql.Result, error) {
	switch {
	case stmt != nil && q.tx != nil:
		return q.tx.StmtContext(ctx, stmt).ExecContext(ctx, args...)
	case stmt != nil:
		return stmt.ExecContext(ctx, args...)
	default:
		return q.db.ExecContext(ctx, query, args...)
	}
}

func (q *Queries) query(ctx context.Context, stmt *sql.Stmt, query string, args ...interface{}) (*sql.Rows, error) {
	switch {
	case stmt != nil && q.tx != nil:
		return q.tx.StmtContext(ctx, stmt).QueryContext(ctx, args...)
	case stmt != nil:
		return stmt.QueryContext(ctx, args...)
	default:
		return q.db.QueryContext(ctx, query, args...)
	}
}

func (q *Queries) queryRow(ctx context.Context, stmt *sql.Stmt, query string, args ...interface{}) *sql.Row {
	switch {
	case stmt != nil && q.tx != nil:
		return q.tx.StmtContext(ctx, stmt).QueryRowContext(ctx, args...)
	case stmt != nil:
		return stmt.QueryRowContext(ctx, args...)
	default:
		return q.db.QueryRowContext(ctx, query, args...)
	}
}

type Queries struct {
	db                                             DBTX
	tx                                             *sql.Tx
	countGroupStmt                                 *sql.Stmt
	deleteOldMarkersStmt                           *sql.Stmt
	findActiveRunsStmt                             *sql.Stmt
	insertMarkerStmt                               *sql.Stmt
	markJobRunsAttemptedByIdStmt                   *sql.Stmt
	markJobRunsFailedByIdStmt                      *sql.Stmt
	markJobRunsReturnedByIdStmt                    *sql.Stmt
	markJobRunsRunningByIdStmt                     *sql.Stmt
	markJobRunsSucceededByIdStmt                   *sql.Stmt
	markJobsCancelRequestedByIdStmt                *sql.Stmt
	markJobsCancelRequestedBySetAndQueuedStateStmt *sql.Stmt
	markJobsCancelledByIdStmt                      *sql.Stmt
	markJobsFailedByIdStmt                         *sql.Stmt
	markJobsSucceededByIdStmt                      *sql.Stmt
	markRunsCancelledByJobIdStmt                   *sql.Stmt
	selectAllExecutorsStmt                         *sql.Stmt
	selectAllJobIdsStmt                            *sql.Stmt
	selectAllMarkersStmt                           *sql.Stmt
	selectAllRunErrorsStmt                         *sql.Stmt
	selectAllRunIdsStmt                            *sql.Stmt
	selectExecutorUpdateTimesStmt                  *sql.Stmt
	selectJobsForExecutorStmt                      *sql.Stmt
	selectNewJobsStmt                              *sql.Stmt
	selectNewRunsStmt                              *sql.Stmt
	selectNewRunsForJobsStmt                       *sql.Stmt
	selectRunErrorsByIdStmt                        *sql.Stmt
	selectUpdatedJobsStmt                          *sql.Stmt
	updateJobPriorityByIdStmt                      *sql.Stmt
	updateJobPriorityByJobSetStmt                  *sql.Stmt
	upsertExecutorStmt                             *sql.Stmt
}

func (q *Queries) WithTx(tx *sql.Tx) *Queries {
	return &Queries{
		db:                              tx,
		tx:                              tx,
		countGroupStmt:                  q.countGroupStmt,
		deleteOldMarkersStmt:            q.deleteOldMarkersStmt,
		findActiveRunsStmt:              q.findActiveRunsStmt,
		insertMarkerStmt:                q.insertMarkerStmt,
		markJobRunsAttemptedByIdStmt:    q.markJobRunsAttemptedByIdStmt,
		markJobRunsFailedByIdStmt:       q.markJobRunsFailedByIdStmt,
		markJobRunsReturnedByIdStmt:     q.markJobRunsReturnedByIdStmt,
		markJobRunsRunningByIdStmt:      q.markJobRunsRunningByIdStmt,
		markJobRunsSucceededByIdStmt:    q.markJobRunsSucceededByIdStmt,
		markJobsCancelRequestedByIdStmt: q.markJobsCancelRequestedByIdStmt,
		markJobsCancelRequestedBySetAndQueuedStateStmt: q.markJobsCancelRequestedBySetAndQueuedStateStmt,
		markJobsCancelledByIdStmt:                      q.markJobsCancelledByIdStmt,
		markJobsFailedByIdStmt:                         q.markJobsFailedByIdStmt,
		markJobsSucceededByIdStmt:                      q.markJobsSucceededByIdStmt,
		markRunsCancelledByJobIdStmt:                   q.markRunsCancelledByJobIdStmt,
		selectAllExecutorsStmt:                         q.selectAllExecutorsStmt,
		selectAllJobIdsStmt:                            q.selectAllJobIdsStmt,
		selectAllMarkersStmt:                           q.selectAllMarkersStmt,
		selectAllRunErrorsStmt:                         q.selectAllRunErrorsStmt,
		selectAllRunIdsStmt:                            q.selectAllRunIdsStmt,
		selectExecutorUpdateTimesStmt:                  q.selectExecutorUpdateTimesStmt,
		selectJobsForExecutorStmt:                      q.selectJobsForExecutorStmt,
		selectNewJobsStmt:                              q.selectNewJobsStmt,
		selectNewRunsStmt:                              q.selectNewRunsStmt,
		selectNewRunsForJobsStmt:                       q.selectNewRunsForJobsStmt,
		selectRunErrorsByIdStmt:                        q.selectRunErrorsByIdStmt,
		selectUpdatedJobsStmt:                          q.selectUpdatedJobsStmt,
		updateJobPriorityByIdStmt:                      q.updateJobPriorityByIdStmt,
		updateJobPriorityByJobSetStmt:                  q.updateJobPriorityByJobSetStmt,
		upsertExecutorStmt:                             q.upsertExecutorStmt,
	}
}
