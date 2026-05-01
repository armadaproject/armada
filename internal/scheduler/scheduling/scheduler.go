package scheduling

import (
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
)

type SchedulerCoordinator struct {
	async          bool
	schedulingAlgo SchedulingAlgo
	jobDb          *jobdb.JobDb

	// Make atomic
	currentResult *scheduleResult
}

type scheduleResult struct {
	schedulerResult *SchedulerResult
	err             error
}

// Issues
//
// Metrics
//  - When do we record metrics?
//  - How do we record failures?
// Errors
//  - Should we immediately retry on error or wait for the error to be consumed?
// Job price updates
//  - As they aren't set as jobs are ingested, we could have jobs with no price data yet
//  - This struct should never update the jobdb
//  - Options
//  -  - Let the pricing be updated async (possibly exclude those with no pricing info from being scheduled)
//  -  - Sort out the jobdb to set price info as jobs come in (how to handle startup pricing?)

func (sc *SchedulerCoordinator) cycle() {
	if sc.currentResult != nil {
		return
	}
	// TODO sort background
	ctx := armadacontext.Background()

	txn := sc.jobDb.WritableSnapshot()
	result, err := sc.schedulingAlgo.Schedule(ctx, nil, txn)
	sc.currentResult = &scheduleResult{schedulerResult: result, err: err}
}

func (sc *SchedulerCoordinator) Schedule(ctx *armadacontext.Context, resourceUnits map[string]internaltypes.ResourceList, txn *jobdb.Txn) (*SchedulerResult, error) {
	if !sc.async {
		return sc.schedulingAlgo.Schedule(ctx, resourceUnits, txn)
	}

	currentResult := sc.currentResult
	if currentResult != nil {
		sc.currentResult = nil
		return currentResult.schedulerResult, currentResult.err
	}

	return nil, nil
}

func (sc *SchedulerCoordinator) calculateReconciledResult(txn *jobdb.Txn, currentResult *scheduleResult) (*SchedulerResult, error) {
	if currentResult.err != nil {
		return currentResult.schedulerResult, currentResult.err
	}

	txn.GetById()

	for _, poolResult := range currentResult.schedulerResult.PoolResults {
		poolResult.SchedulingResult.SchedulingContext.MarkUnscheduled
	}

}
