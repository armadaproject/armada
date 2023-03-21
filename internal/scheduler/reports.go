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
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// SchedulingContextRepository stores scheduling contexts associated with recent scheduling attempts.
type SchedulingContextRepository struct {
	// Maps executor id to *SchedulingContext.
	schedulingContextByExecutor SchedulingContextByExecutor
	// Maps queue name to QueueSchedulingContextByExecutor.
	queueSchedulingContextByExecutorByQueue map[string]QueueSchedulingContextByExecutor
	// Maps job id to JobSchedulingContextByExecutor.
	// We limit the number of job contexts to store to control memory usage.
	jobSchedulingContextByExecutorByJobId *lru.Cache
	// Protects the fields in this struct from writing concurrent with reading.
	// (Concurrent reading from maps is safe if there are no concurrent writes.)
	rw sync.RWMutex
}

func NewSchedulingContextRepository(maxJobSchedulingContexts uint) (*SchedulingContextRepository, error) {
	jobSchedulingContextByExecutorByJobId, err := lru.New(int(maxJobSchedulingContexts))
	if err != nil {
		return nil, err
	}
	return &SchedulingContextRepository{
		schedulingContextByExecutor:             make(SchedulingContextByExecutor),
		queueSchedulingContextByExecutorByQueue: make(map[string]QueueSchedulingContextByExecutor),
		jobSchedulingContextByExecutorByJobId:   jobSchedulingContextByExecutorByJobId,
	}, nil
}

func (repo *SchedulingContextRepository) AddSchedulingContext(sctx *SchedulingContext) {
	queueSchedulingContextByQueue, jobSchedulingContextByJobId := extractQueueAndJobContexts(sctx)
	repo.rw.Lock()
	defer repo.rw.Unlock()
	for jobId, jctx := range jobSchedulingContextByJobId {
		previous, ok, _ := repo.jobSchedulingContextByExecutorByJobId.PeekOrAdd(
			jobId,
			JobSchedulingContextByExecutor{sctx.ExecutorId: jctx},
		)
		if ok {
			jobSchedulingContextByExecutor := previous.(JobSchedulingContextByExecutor)
			jobSchedulingContextByExecutor[sctx.ExecutorId] = jctx
			repo.jobSchedulingContextByExecutorByJobId.Add(jobId, jobSchedulingContextByExecutor)
		}
	}
	for queue, qctx := range queueSchedulingContextByQueue {
		if previous := repo.queueSchedulingContextByExecutorByQueue[queue]; previous != nil {
			previous[sctx.ExecutorId] = qctx
		} else {
			repo.queueSchedulingContextByExecutorByQueue[queue] = QueueSchedulingContextByExecutor{
				sctx.ExecutorId: qctx,
			}
		}
	}
	repo.schedulingContextByExecutor[sctx.ExecutorId] = sctx
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
	schedulingContextByExecutor := repo.GetSchedulingContextByExecutor()
	if schedulingContextByExecutor == nil {
		return &schedulerobjects.SchedulingReport{
			Report: "no recent scheduling attempts",
		}, nil
	}
	return &schedulerobjects.SchedulingReport{
		Report: schedulingContextByExecutor.String(),
	}, nil
}

func (repo *SchedulingContextRepository) GetQueueReport(_ context.Context, queue *schedulerobjects.Queue) (*schedulerobjects.QueueReport, error) {
	queueSchedulingContextByExecutor, ok := repo.GetQueueSchedulingContextByExecutor(queue.Name)
	if queueSchedulingContextByExecutor == nil || !ok {
		return &schedulerobjects.QueueReport{
			Report: fmt.Sprintf("queue %s has not been considered for scheduling recently", queue.Name),
		}, nil
	}
	return &schedulerobjects.QueueReport{
		Report: queueSchedulingContextByExecutor.String(),
	}, nil
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
	schedulingContextByExecutor := repo.schedulingContextByExecutor
	return schedulingContextByExecutor
}

func (repo *SchedulingContextRepository) GetQueueSchedulingContextByExecutor(queueName string) (QueueSchedulingContextByExecutor, bool) {
	repo.rw.RLock()
	defer repo.rw.RUnlock()
	if queueSchedulingContextByExecutor, ok := repo.queueSchedulingContextByExecutorByQueue[queueName]; ok {
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
		fmt.Fprintf(w, "Executor %s:\n", executorId)
		fmt.Fprint(w, indent.String("\t", jctx.String()))
	}
	w.Flush()
	return sb.String()
}
