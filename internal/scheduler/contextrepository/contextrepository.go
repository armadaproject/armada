package contextrepository

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"text/tabwriter"

	lru "github.com/hashicorp/golang-lru"
	"github.com/oklog/ulid"
	"github.com/openconfig/goyang/pkg/indent"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// SchedulingContextRepository stores scheduling contexts associated with recent scheduling rounds.
type SchedulingContextRepository struct {
	// For the n most recently referenced jobs, where n is a parameter, the most recent SchedulingContext for each executor.
	// A SchedulingContext references a job if the job is either scheduled or preempted in the corresponding scheduling round.
	// The number of contexts stored is limited to n to control memory usage.
	mostRecentSchedulingContextByExecutorAndJobId *lru.Cache
	// Collection of scheduling contexts indexed by executor and queue.
	// Stored as separate struct such that we can atomically swap in a new internally consistent one.
	mostRecentSchedulingContexts atomic.Pointer[schedulingContexts]
}

type schedulingContexts struct {
	// The most recent SchedulingContext for each executor.
	mostRecentSchedulingContextByExecutor SchedulingContextByExecutor
	// The most recent SchedulingContext for each executor where a non-zero amount of resources were scheduled.
	mostRecentSuccessfulSchedulingContextByExecutor SchedulingContextByExecutor
	// The most recent SchedulingContext for each executor where a non-zero amount of resources were preempted.
	mostRecentPreemptingSchedulingContextByExecutor SchedulingContextByExecutor

	// The most recent SchedulingContext for each queue.
	mostRecentSchedulingContextByExecutorAndQueue map[string]SchedulingContextByExecutor
	// The most recent SchedulingContext for each queue  where a non-zero amount of resources were scheduled.
	mostRecentSuccessfulSchedulingContextByExecutorAndQueue map[string]SchedulingContextByExecutor
	// The most recent SchedulingContext for each queue  where a non-zero amount of resources were preempted.
	mostRecentPreemptingSchedulingContextByExecutorAndQueue map[string]SchedulingContextByExecutor

	// All executor ids seen.
	executorIds map[string]bool
	// All executors ids seen in sorted order.
	sortedExecutorIds []string
}

type SchedulingContextByExecutor map[string]*schedulercontext.SchedulingContext

func (sctxByExecutor SchedulingContextByExecutor) add(sctx *schedulercontext.SchedulingContext) SchedulingContextByExecutor {
	if sctxByExecutor == nil {
		sctxByExecutor = make(SchedulingContextByExecutor)
	}
	sctxByExecutor[sctx.ExecutorId] = sctx
	return sctxByExecutor
}

func NewSchedulingContextRepository(maxJobSchedulingContextsPerExecutor uint) (*SchedulingContextRepository, error) {
	jobSchedulingContextByExecutorAndJobId, err := lru.New(int(maxJobSchedulingContextsPerExecutor))
	if err != nil {
		return nil, err
	}
	rv := &SchedulingContextRepository{
		mostRecentSchedulingContextByExecutorAndJobId: jobSchedulingContextByExecutorAndJobId,
	}
	rv.mostRecentSchedulingContexts.Store(&schedulingContexts{})
	return rv, nil
}

// AddSchedulingContext adds a scheduling context to the repo.
// It also extracts the queue and job scheduling contexts it contains and stores those separately.
//
// It's safe to call this method concurrently with itself and with methods getting contexts from the repo.
// It's not safe to mutate contexts once they've been provided to this method.
func (repo *SchedulingContextRepository) AddSchedulingContext(ctx context.Context, sctx *schedulercontext.SchedulingContext) bool {
	for _, qctx := range sctx.QueueSchedulingContexts {
		for _, jctx := range qctx.SuccessfulJobSchedulingContexts {
			repo.mostRecentSchedulingContextByExecutorAndJobId.Add(jctx.JobId, sctx)
		}
		for _, jctx := range qctx.UnsuccessfulJobSchedulingContexts {
			repo.mostRecentSchedulingContextByExecutorAndJobId.Add(jctx.JobId, sctx)
		}
	}
	for {
		old := repo.mostRecentSchedulingContexts.Load()
		new := old.DeepCopy()
		new.AddSchedulingContext(sctx)
		if ok := repo.mostRecentSchedulingContexts.CompareAndSwap(old, new); ok {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		default:
		}
	}
}

func (old *schedulingContexts) DeepCopy() *schedulingContexts {
	new := *old
	new.mostRecentSchedulingContextByExecutor = maps.Clone(new.mostRecentSchedulingContextByExecutor)
	new.mostRecentSuccessfulSchedulingContextByExecutor = maps.Clone(new.mostRecentSuccessfulSchedulingContextByExecutor)
	new.mostRecentPreemptingSchedulingContextByExecutor = maps.Clone(new.mostRecentPreemptingSchedulingContextByExecutor)
	new.mostRecentSchedulingContextByExecutorAndQueue = maps.Clone(new.mostRecentSchedulingContextByExecutorAndQueue)
	new.mostRecentSuccessfulSchedulingContextByExecutorAndQueue = maps.Clone(new.mostRecentSuccessfulSchedulingContextByExecutorAndQueue)
	new.mostRecentPreemptingSchedulingContextByExecutorAndQueue = maps.Clone(new.mostRecentPreemptingSchedulingContextByExecutorAndQueue)
	if new.mostRecentSchedulingContextByExecutorAndQueue == nil {
		new.mostRecentSchedulingContextByExecutorAndQueue = make(map[string]SchedulingContextByExecutor)
	}
	if new.mostRecentSuccessfulSchedulingContextByExecutorAndQueue == nil {
		new.mostRecentSuccessfulSchedulingContextByExecutorAndQueue = make(map[string]SchedulingContextByExecutor)
	}
	if new.mostRecentPreemptingSchedulingContextByExecutorAndQueue == nil {
		new.mostRecentPreemptingSchedulingContextByExecutorAndQueue = make(map[string]SchedulingContextByExecutor)
	}
	new.executorIds = maps.Clone(new.executorIds)
	if new.executorIds == nil {
		new.executorIds = make(map[string]bool)
	}
	new.sortedExecutorIds = slices.Clone(new.sortedExecutorIds)
	return &new
}

func (r *schedulingContexts) AddSchedulingContext(sctx *schedulercontext.SchedulingContext) {
	isSuccessful := false
	isPreempting := false
	for _, qctx := range sctx.QueueSchedulingContexts {
		r.mostRecentSchedulingContextByExecutorAndQueue[qctx.Queue] = r.mostRecentSchedulingContextByExecutorAndQueue[qctx.Queue].add(sctx)
		if len(qctx.SuccessfulJobSchedulingContexts) > 0 {
			isSuccessful = true
			r.mostRecentSuccessfulSchedulingContextByExecutorAndQueue[qctx.Queue] = r.mostRecentSuccessfulSchedulingContextByExecutorAndQueue[qctx.Queue].add(sctx)
		}
		if len(qctx.EvictedJobIds) > 0 {
			isPreempting = true
			r.mostRecentPreemptingSchedulingContextByExecutorAndQueue[qctx.Queue] = r.mostRecentPreemptingSchedulingContextByExecutorAndQueue[qctx.Queue].add(sctx)
		}
	}
	r.mostRecentSchedulingContextByExecutor = r.mostRecentSchedulingContextByExecutor.add(sctx)
	if isSuccessful {
		r.mostRecentSuccessfulSchedulingContextByExecutor = r.mostRecentSuccessfulSchedulingContextByExecutor.add(sctx)
	}
	if isPreempting {
		r.mostRecentPreemptingSchedulingContextByExecutor = r.mostRecentPreemptingSchedulingContextByExecutor.add(sctx)
	}
	r.executorIds[sctx.ExecutorId] = true
	r.sortedExecutorIds = maps.Keys(r.executorIds)
	slices.Sort(r.sortedExecutorIds)
}

// GetSchedulingReport is a gRPC endpoint for querying scheduler reports.
// TODO: Further separate this from internal contexts.
func (repo *SchedulingContextRepository) GetSchedulingReport(_ context.Context, req *schedulerobjects.SchedulingReportRequest) (*schedulerobjects.SchedulingReport, error) {
	return &schedulerobjects.SchedulingReport{
		Report: repo.getSchedulingReportString(),
	}, nil
}

func (repo *SchedulingContextRepository) getSchedulingReportString() string {
	current := repo.mostRecentSchedulingContexts.Load()
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	for _, executorId := range current.sortedExecutorIds {
		fmt.Fprintf(w, "%s:\n", executorId)
		if sctx := current.mostRecentSchedulingContextByExecutor[executorId]; sctx != nil {
			fmt.Fprint(w, indent.String("\t", "Most recent invocation:\n"))
			fmt.Fprint(w, indent.String("\t\t", sctx.String()))
		} else {
			fmt.Fprint(w, indent.String("\t", "Most recent invocation: none\n"))
		}
		if sctx := current.mostRecentSuccessfulSchedulingContextByExecutor[executorId]; sctx != nil {
			fmt.Fprint(w, indent.String("\t", "Most recent successful invocation:\n"))
			fmt.Fprint(w, indent.String("\t\t", sctx.String()))
		} else {
			fmt.Fprint(w, indent.String("\t", "Most recent successful invocation: none\n"))
		}
		if sctx := current.mostRecentPreemptingSchedulingContextByExecutor[executorId]; sctx != nil {
			fmt.Fprint(w, indent.String("\t", "Most recent preempting invocation:\n"))
			fmt.Fprint(w, indent.String("\t\t", sctx.String()))
		} else {
			fmt.Fprint(w, indent.String("\t", "Most recent preempting invocation: none\n"))
		}
	}
	w.Flush()
	return sb.String()
}

// GetQueueReport is a gRPC endpoint for querying queue reports.
// TODO: Further separate this from internal contexts.
func (repo *SchedulingContextRepository) GetQueueReport(_ context.Context, req *schedulerobjects.QueueReportRequest) (*schedulerobjects.QueueReport, error) {
	queueName := strings.TrimSpace(req.Queue)
	return &schedulerobjects.QueueReport{
		Report: repo.getQueueReportString(queueName),
	}, nil
}

func (repo *SchedulingContextRepository) getQueueReportString(queue string) string {
	current := repo.mostRecentSchedulingContexts.Load()
	mostRecentSchedulingContextByExecutor := current.mostRecentSchedulingContextByExecutorAndQueue[queue]
	mostRecentSuccessfulSchedulingContextByExecutor := current.mostRecentSuccessfulSchedulingContextByExecutorAndQueue[queue]
	mostRecentPreemptingSchedulingContextByExecutor := current.mostRecentPreemptingSchedulingContextByExecutorAndQueue[queue]
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	for _, executorId := range current.sortedExecutorIds {
		fmt.Fprintf(w, "%s:\n", executorId)
		if sctx := mostRecentSchedulingContextByExecutor[executorId]; sctx != nil {
			if qctx := sctx.QueueSchedulingContexts[queue]; qctx != nil {
				fmt.Fprint(w, indent.String("\t", "Most recent invocation:\n"))
				fmt.Fprint(w, indent.String("\t\t", qctx.String()))
			} else {
				fmt.Fprint(w, indent.String("\t", "Most recent invocation: none\n"))
			}
		} else {
			fmt.Fprint(w, indent.String("\t", "Most recent invocation: none\n"))
		}
		if sctx := mostRecentSuccessfulSchedulingContextByExecutor[executorId]; sctx != nil {
			if qctx := sctx.QueueSchedulingContexts[queue]; qctx != nil {
				fmt.Fprint(w, indent.String("\t", "Most recent successful invocation:\n"))
				fmt.Fprint(w, indent.String("\t\t", qctx.String()))
			} else {
				fmt.Fprint(w, indent.String("\t", "Most recent successful invocation: none\n"))
			}
		} else {
			fmt.Fprint(w, indent.String("\t", "Most recent successful invocation: none\n"))
		}
		if sctx := mostRecentPreemptingSchedulingContextByExecutor[executorId]; sctx != nil {
			if qctx := sctx.QueueSchedulingContexts[queue]; qctx != nil {
				fmt.Fprint(w, indent.String("\t", "Most recent preempting invocation:\n"))
				fmt.Fprint(w, indent.String("\t\t", qctx.String()))
			} else {
				fmt.Fprint(w, indent.String("\t", "Most recent preempting invocation: none\n"))
			}
		} else {
			fmt.Fprint(w, indent.String("\t", "Most recent preempting invocation: none\n"))
		}
	}
	w.Flush()
	return sb.String()
}

// GetJobReport is a gRPC endpoint for querying job reports.
// TODO: Further separate this from internal contexts.
func (repo *SchedulingContextRepository) GetJobReport(_ context.Context, req *schedulerobjects.JobReportRequest) (*schedulerobjects.JobReport, error) {
	key := strings.TrimSpace(req.JobId)
	if _, err := ulid.Parse(key); err != nil {
		return nil, &armadaerrors.ErrInvalidArgument{
			Name:    "jobId",
			Value:   req.JobId,
			Message: fmt.Sprintf("%s is not a valid jobId", req.JobId),
		}
	}
	return &schedulerobjects.JobReport{
		Report: repo.getJobReportString(key),
	}, nil
}

func (repo *SchedulingContextRepository) getJobReportString(jobId string) string {
	current := repo.mostRecentSchedulingContexts.Load()
	mostRecentSchedulingContextByExecutor, _ := repo.GetMostRecentJobSchedulingContextByExecutor(jobId)
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	for _, executorId := range current.sortedExecutorIds {
		if sctx := mostRecentSchedulingContextByExecutor[executorId]; sctx != nil {
			var jctx *schedulercontext.JobSchedulingContext
			for _, qctx := range sctx.QueueSchedulingContexts {
				if jctx = qctx.SuccessfulJobSchedulingContexts[jobId]; jctx != nil {
					break
				} else if jctx = qctx.UnsuccessfulJobSchedulingContexts[jobId]; jctx != nil {
					break
				}
			}
			if jctx != nil {
				fmt.Fprintf(w, "%s:\n", executorId)
				fmt.Fprint(w, indent.String("\t", jctx.String()))
			} else {
				fmt.Fprintf(w, "%s: no recent attempt\n", executorId)
			}
		} else {
			fmt.Fprintf(w, "%s: no recent attempt\n", executorId)
		}
	}
	w.Flush()
	return sb.String()
}

func (repo *SchedulingContextRepository) GetMostRecentJobSchedulingContextByExecutor(jobId string) (SchedulingContextByExecutor, bool) {
	if v, ok := repo.mostRecentSchedulingContextByExecutorAndJobId.Get(jobId); ok {
		schedulingContextByExecutor := v.(SchedulingContextByExecutor)
		return schedulingContextByExecutor, true
	} else {
		return nil, false
	}
}
