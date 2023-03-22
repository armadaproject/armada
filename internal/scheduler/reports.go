package scheduler

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"text/tabwriter"

	"github.com/gogo/protobuf/types"
	lru "github.com/hashicorp/golang-lru"
	"github.com/openconfig/goyang/pkg/indent"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// SchedulingContextRepository stores scheduling contexts associated with recent scheduling attempts.
//
// TODO: Add async way to add reports.
type SchedulingContextRepository struct {
	// Maps executor id to *SchedulingContext.
	// The most recent attempt.
	mostRecentSchedulingContextByExecutor SchedulingContextByExecutor
	// The most recent attempt where a non-zero amount of resources where scheduled.
	mostRecentSuccessfulSchedulingContextByExecutor SchedulingContextByExecutor
	// Maps queue name to QueueSchedulingContextByExecutor.
	// The most recent attempt.
	mostRecentQueueSchedulingContextByExecutorByQueue map[string]QueueSchedulingContextByExecutor
	// The most recent attempt where a non-zero amount of resources where scheduled.
	mostRecentSuccessfulQueueSchedulingContextByExecutorByQueue map[string]QueueSchedulingContextByExecutor
	// Maps job id to JobSchedulingContextByExecutor.
	// We limit the number of job contexts to store to control memory usage.
	jobSchedulingContextByExecutorByJobId *lru.Cache
	// Store all executor ids seen so far in a set.
	// Used to ensure all executors are included in reports.
	executorIds map[string]bool
	// All executors in sorted order.
	sortedExecutorIds []string
	// Protects the fields in this struct from writing concurrent with reading.
	// (Concurrent reading from maps is safe if there are no concurrent writes.)
	rw sync.RWMutex
}

func NewSchedulingContextRepository(maxJobSchedulingContextsPerExecutor uint) (*SchedulingContextRepository, error) {
	jobSchedulingContextByExecutorByJobId, err := lru.New(int(maxJobSchedulingContextsPerExecutor))
	if err != nil {
		return nil, err
	}
	return &SchedulingContextRepository{
		mostRecentSchedulingContextByExecutor:                       make(SchedulingContextByExecutor),
		mostRecentSuccessfulSchedulingContextByExecutor:             make(SchedulingContextByExecutor),
		mostRecentQueueSchedulingContextByExecutorByQueue:           make(map[string]QueueSchedulingContextByExecutor),
		mostRecentSuccessfulQueueSchedulingContextByExecutorByQueue: make(map[string]QueueSchedulingContextByExecutor),
		jobSchedulingContextByExecutorByJobId:                       jobSchedulingContextByExecutorByJobId,
		executorIds:                                                 make(map[string]bool),
	}, nil
}

func (repo *SchedulingContextRepository) AddSchedulingContext(sctx *SchedulingContext) error {
	queueSchedulingContextByQueue, jobSchedulingContextByJobId := extractQueueAndJobContexts(sctx)
	repo.rw.Lock()
	defer repo.rw.Unlock()
	for _, jctx := range jobSchedulingContextByJobId {
		if err := repo.addJobSchedulingContext(jctx); err != nil {
			return err
		}
	}
	for _, qctx := range queueSchedulingContextByQueue {
		if err := repo.addQueueSchedulingContext(qctx); err != nil {
			return err
		}
	}

	repo.mostRecentSchedulingContextByExecutor[sctx.ExecutorId] = sctx
	if !sctx.ScheduledResourcesByPriority.IsZero() {
		repo.mostRecentSuccessfulSchedulingContextByExecutor[sctx.ExecutorId] = sctx
	}

	n := len(repo.executorIds)
	repo.executorIds[sctx.ExecutorId] = true
	if len(repo.executorIds) != n {
		repo.sortedExecutorIds = append(repo.sortedExecutorIds, sctx.ExecutorId)
		slices.Sort(repo.sortedExecutorIds)
	}
	return nil
}

// Should only be called from AddSchedulingContext to avoid concurrency/locking issues.
func (repo *SchedulingContextRepository) addQueueSchedulingContext(qctx *QueueSchedulingContext) error {
	if qctx.ExecutorId == "" {
		return errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "ExecutorId",
			Value:   "",
			Message: "received empty executorId",
		})
	}
	if qctx.Queue == "" {
		return errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "Queue",
			Value:   "",
			Message: "received empty queue name",
		})
	}
	if previous := repo.mostRecentQueueSchedulingContextByExecutorByQueue[qctx.Queue]; previous != nil {
		previous[qctx.ExecutorId] = qctx
	} else {
		repo.mostRecentQueueSchedulingContextByExecutorByQueue[qctx.Queue] = QueueSchedulingContextByExecutor{
			qctx.ExecutorId: qctx,
		}
	}
	if !qctx.ScheduledResourcesByPriority.IsZero() {
		if previous := repo.mostRecentSuccessfulQueueSchedulingContextByExecutorByQueue[qctx.Queue]; previous != nil {
			previous[qctx.ExecutorId] = qctx
		} else {
			repo.mostRecentSuccessfulQueueSchedulingContextByExecutorByQueue[qctx.Queue] = QueueSchedulingContextByExecutor{
				qctx.ExecutorId: qctx,
			}
		}
	}
	return nil
}

// Should only be called from AddSchedulingContext to avoid concurrency/locking issues.
func (repo *SchedulingContextRepository) addJobSchedulingContext(jctx *JobSchedulingContext) error {
	if jctx.ExecutorId == "" {
		return errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "ExecutorId",
			Value:   "",
			Message: "received empty executorId",
		})
	}
	if jctx.JobId == "" {
		return errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "JobId",
			Value:   "",
			Message: "received empty jobId",
		})
	}
	previous, ok, _ := repo.jobSchedulingContextByExecutorByJobId.PeekOrAdd(
		jctx.JobId,
		JobSchedulingContextByExecutor{jctx.ExecutorId: jctx},
	)
	if ok {
		jobSchedulingContextByExecutor := previous.(JobSchedulingContextByExecutor)
		jobSchedulingContextByExecutor[jctx.ExecutorId] = jctx
		repo.jobSchedulingContextByExecutorByJobId.Add(jctx.JobId, jobSchedulingContextByExecutor)
	}
	return nil
}

// NormaliseSchedulingContext extracts the job and queue scheduling contexts from the scheduling context,
// and returns those separately.
func extractQueueAndJobContexts(sctx *SchedulingContext) (map[string]*QueueSchedulingContext, map[string]*JobSchedulingContext) {
	queueSchedulingContextByQueue := make(map[string]*QueueSchedulingContext)
	jobSchedulingContextByJobId := make(map[string]*JobSchedulingContext)
	for queue, qctx := range sctx.QueueSchedulingContexts {
		for jobId, jctx := range qctx.SuccessfulJobSchedulingContexts {
			jobSchedulingContextByJobId[jobId] = jctx
		}
		for jobId, jctx := range qctx.UnsuccessfulJobSchedulingContexts {
			jobSchedulingContextByJobId[jobId] = jctx
		}
		queueSchedulingContextByQueue[queue] = qctx
	}
	return queueSchedulingContextByQueue, jobSchedulingContextByJobId
}

func (repo *SchedulingContextRepository) GetSchedulingReport(_ context.Context, _ *types.Empty) (*schedulerobjects.SchedulingReport, error) {
	return &schedulerobjects.SchedulingReport{
		Report: repo.getSchedulingReportString(),
	}, nil
}

func (repo *SchedulingContextRepository) getSchedulingReportString() string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	for _, executorId := range repo.sortedExecutorIds {
		fmt.Fprintf(w, "%s:\n", executorId)
		sctx := repo.mostRecentSchedulingContextByExecutor[executorId]
		if sctx != nil {
			fmt.Fprint(w, indent.String("\t", "Most recent attempt:\n"))
			fmt.Fprint(w, indent.String("\t\t", sctx.String()))
		} else {
			fmt.Fprint(w, indent.String("\t", "Most recent attempt: none\n"))
		}
		sctx = repo.mostRecentSuccessfulSchedulingContextByExecutor[executorId]
		if sctx != nil {
			fmt.Fprint(w, indent.String("\t", "Most recent successful attempt:\n"))
			fmt.Fprint(w, indent.String("\t\t", sctx.String()))
		} else {
			fmt.Fprint(w, indent.String("\t", "Most recent successful attempt: none\n"))
		}
	}
	w.Flush()
	return sb.String()
}

func (repo *SchedulingContextRepository) GetQueueReport(_ context.Context, queue *schedulerobjects.Queue) (*schedulerobjects.QueueReport, error) {
	queueName := strings.TrimSpace(queue.Name)
	return &schedulerobjects.QueueReport{
		Report: repo.getQueueReportString(queueName),
	}, nil
}

func (repo *SchedulingContextRepository) getQueueReportString(queue string) string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	mostRecentQueueSchedulingContextByExecutor := repo.mostRecentQueueSchedulingContextByExecutorByQueue[queue]
	mostRecentSuccessfulQueueSchedulingContextByExecutor := repo.mostRecentSuccessfulQueueSchedulingContextByExecutorByQueue[queue]
	for _, executorId := range repo.sortedExecutorIds {
		fmt.Fprintf(w, "%s:\n", executorId)
		qctx := mostRecentQueueSchedulingContextByExecutor[executorId]
		if qctx != nil {
			fmt.Fprint(w, indent.String("\t", "Most recent attempt:\n"))
			fmt.Fprint(w, indent.String("\t\t", qctx.String()))
		} else {
			fmt.Fprint(w, indent.String("\t", "Most recent attempt: none\n"))
		}
		qctx = mostRecentSuccessfulQueueSchedulingContextByExecutor[executorId]
		if qctx != nil {
			fmt.Fprint(w, indent.String("\t", "Most recent successful attempt:\n"))
			fmt.Fprint(w, indent.String("\t\t", qctx.String()))
		} else {
			fmt.Fprint(w, indent.String("\t", "Most recent successful attempt: none\n"))
		}
	}
	w.Flush()
	return sb.String()
}

func (repo *SchedulingContextRepository) GetJobReport(_ context.Context, jobId *schedulerobjects.JobId) (*schedulerobjects.JobReport, error) {
	jobSchedulingContextByExecutor, ok := repo.GetJobSchedulingContextByExecutor(jobId.Id)
	if jobSchedulingContextByExecutor == nil || !ok {
		return &schedulerobjects.JobReport{
			Report: fmt.Sprintf("job %s has not been considered for scheduling recently", jobId.Id),
		}, nil
	}
	return &schedulerobjects.JobReport{
		Report: jobSchedulingContextByExecutor.String(),
	}, nil
}

func (repo *SchedulingContextRepository) GetSchedulingContextByExecutor() SchedulingContextByExecutor {
	repo.rw.RLock()
	defer repo.rw.RUnlock()
	schedulingContextByExecutor := repo.mostRecentSchedulingContextByExecutor
	return schedulingContextByExecutor
}

func (repo *SchedulingContextRepository) GetQueueSchedulingContextByExecutor(queueName string) (QueueSchedulingContextByExecutor, bool) {
	repo.rw.RLock()
	defer repo.rw.RUnlock()
	if queueSchedulingContextByExecutor, ok := repo.mostRecentQueueSchedulingContextByExecutorByQueue[queueName]; ok {
		return queueSchedulingContextByExecutor, true
	} else {
		return nil, false
	}
}

func (repo *SchedulingContextRepository) GetJobSchedulingContextByExecutor(jobId string) (JobSchedulingContextByExecutor, bool) {
	repo.rw.RLock()
	defer repo.rw.RUnlock()
	if v, ok := repo.jobSchedulingContextByExecutorByJobId.Get(jobId); ok {
		jobSchedulingContextByExecutor := v.(JobSchedulingContextByExecutor)
		return jobSchedulingContextByExecutor, true
	} else {
		return nil, false
	}
}

type (
	SchedulingContextByExecutor      map[string]*SchedulingContext
	QueueSchedulingContextByExecutor map[string]*QueueSchedulingContext
	JobSchedulingContextByExecutor   map[string]*JobSchedulingContext
)

func (m SchedulingContextByExecutor) String() string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	executorIds := maps.Keys(m)
	slices.Sort(executorIds)
	for _, executorId := range executorIds {
		sctx := m[executorId]
		fmt.Fprintf(w, "Executor %s:\n", executorId)
		fmt.Fprint(w, indent.String("\t", sctx.String()))
	}
	w.Flush()
	return sb.String()
}

func (m QueueSchedulingContextByExecutor) String() string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	executorIds := maps.Keys(m)
	slices.Sort(executorIds)
	for _, executorId := range executorIds {
		qctx := m[executorId]
		fmt.Fprintf(w, "Executor %s:\n", executorId)
		fmt.Fprint(w, indent.String("\t", qctx.String()))
	}
	w.Flush()
	return sb.String()
}

func (m JobSchedulingContextByExecutor) String() string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	executorIds := maps.Keys(m)
	slices.Sort(executorIds)
	for _, executorId := range executorIds {
		jctx := m[executorId]
		fmt.Fprintf(w, "%s:\n", executorId)
		fmt.Fprint(w, indent.String("\t", jctx.String()))
	}
	w.Flush()
	return sb.String()
}
