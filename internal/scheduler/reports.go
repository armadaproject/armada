package scheduler

import (
	"fmt"
	"sync"

	lru "github.com/hashicorp/golang-lru"
	"golang.org/x/exp/maps"
)

type SchedulingContextByExecutor map[string]*SchedulingContext
type QueueSchedulingContextByExecutor map[string]*QueueSchedulingContext
type JobSchedulingContextByExecutor map[string]*JobSchedulingContext

// SchedulingContextRepository stores scheduling contexts on recent scheduling attempts.
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
	queueSchedulingContextByQueue, jobSchedulingContextByJobId := normaliseSchedulingContext(sctx)
	repo.rw.Lock()
	defer repo.rw.Unlock()
	for jobId, jctx := range jobSchedulingContextByJobId {
		fmt.Println("adding job", jobId)
		fmt.Println("len before", repo.jobSchedulingContextByExecutorByJobId.Len())
		previous, ok, _ := repo.jobSchedulingContextByExecutorByJobId.PeekOrAdd(
			jobId,
			JobSchedulingContextByExecutor{sctx.ExecutorId: jctx},
		)
		if ok {
			fmt.Println("job already exists")
			jobSchedulingContextByExecutor := previous.(JobSchedulingContextByExecutor)
			jobSchedulingContextByExecutor[sctx.ExecutorId] = jctx
			repo.jobSchedulingContextByExecutorByJobId.Add(jobId, jobSchedulingContextByExecutor)
		}
		fmt.Println("len after", repo.jobSchedulingContextByExecutorByJobId.Len())
		// v, ok := repo.jobSchedulingContextByExecutorByJobId.Get(jobId)
		// fmt.Println("getting", ok, v.(JobSchedulingContextByExecutor))
	}
	for queue, qctx := range queueSchedulingContextByQueue {
		if previous := repo.queueSchedulingContextByExecutorByQueue[queue]; previous != nil {
			fmt.Println("adding new queue", queue, "for executor", sctx.ExecutorId)
			previous[sctx.ExecutorId] = qctx
		} else {
			fmt.Println("updating queue", queue, "for executor", sctx.ExecutorId)
			repo.queueSchedulingContextByExecutorByQueue[queue] = QueueSchedulingContextByExecutor{
				sctx.ExecutorId: qctx,
			}
		}
	}
	repo.schedulingContextByExecutor[sctx.ExecutorId] = sctx
}

// NormaliseSchedulingContext extracts the job and queue scheduling contexts from the scheduling context,
// and returns those separately.
func normaliseSchedulingContext(sctx *SchedulingContext) (map[string]*QueueSchedulingContext, map[string]*JobSchedulingContext) {
	queueSchedulingContextByQueue := make(map[string]*QueueSchedulingContext)
	jobSchedulingContextByJobId := make(map[string]*JobSchedulingContext)
	for queue, qctx := range sctx.QueueSchedulingContexts {
		for jobId, jctx := range qctx.SuccessfulJobSchedulingContexts {
			jobSchedulingContextByJobId[jobId] = jctx
			qctx.SuccessfulJobSchedulingContexts[jobId] = nil
		}
		for jobId, jctx := range qctx.UnsuccessfulJobSchedulingContexts {
			jobSchedulingContextByJobId[jobId] = jctx
			qctx.UnsuccessfulJobSchedulingContexts[jobId] = nil
		}
		queueSchedulingContextByQueue[queue] = qctx
		sctx.QueueSchedulingContexts[queue] = nil
	}
	return queueSchedulingContextByQueue, jobSchedulingContextByJobId
}

// func (repo *SchedulingContextRepository) GetQueueReport(ctx context.Context, queue *schedulerobjects.Queue) (*schedulerobjects.QueueReport, error) {
// 	report, ok := repo.GetQueueSchedulingReport(queue.Name)
// 	if !ok {
// 		return nil, &armadaerrors.ErrNotFound{
// 			Type:    "QueueSchedulingReport",
// 			Value:   queue.Name,
// 			Message: "this queue has not been considered for scheduling recently",
// 		}
// 	}
// 	return &schedulerobjects.QueueReport{
// 		Report: report.String(),
// 	}, nil
// }

// func (repo *SchedulingContextRepository) GetJobReport(ctx context.Context, jobId *schedulerobjects.JobId) (*schedulerobjects.JobReport, error) {
// 	jobUuid, err := uuidFromUlidString(jobId.Id)
// 	if err != nil {
// 		return nil, err
// 	}
// 	report, ok := repo.GetJobSchedulingReport(jobUuid)
// 	if !ok {
// 		return nil, &armadaerrors.ErrNotFound{
// 			Type:    "JobSchedulingReport",
// 			Value:   jobId.Id,
// 			Message: "this job has not been considered for scheduling recently",
// 		}
// 	}
// 	return &schedulerobjects.JobReport{
// 		Report: report.String(),
// 	}, nil
// }

func (repo *SchedulingContextRepository) GetSchedulingContextByExecutor() SchedulingContextByExecutor {
	repo.rw.RLock()
	defer repo.rw.RUnlock()
	schedulingContextByExecutor := maps.Clone(repo.schedulingContextByExecutor)
	repo.denormaliseSchedulingContextByExecutor(schedulingContextByExecutor)
	return schedulingContextByExecutor
}

func (repo *SchedulingContextRepository) denormaliseSchedulingContextByExecutor(schedulingContextByExecutor SchedulingContextByExecutor) {
	repo.rw.RLock()
	defer repo.rw.RUnlock()
	for executorId, sctx := range schedulingContextByExecutor {
		for _, qctx := range sctx.QueueSchedulingContexts {
			repo.denormaliseQueueSchedulingContext(QueueSchedulingContextByExecutor{executorId: qctx})
		}
	}
}

func (repo *SchedulingContextRepository) GetQueueSchedulingContextByExecutor(queueName string) (QueueSchedulingContextByExecutor, bool) {
	repo.rw.RLock()
	defer repo.rw.RUnlock()
	if queueSchedulingContextByExecutor, ok := repo.queueSchedulingContextByExecutorByQueue[queueName]; ok {
		repo.denormaliseQueueSchedulingContext(queueSchedulingContextByExecutor)
		return queueSchedulingContextByExecutor, true
	} else {
		return nil, false
	}
}

func (repo *SchedulingContextRepository) denormaliseQueueSchedulingContext(queueSchedulingContextByExecutor QueueSchedulingContextByExecutor) {
	repo.rw.RLock()
	defer repo.rw.RUnlock()
	for executorId, qctx := range queueSchedulingContextByExecutor {
		if qctx == nil {
			continue
		}
		for jobId := range qctx.SuccessfulJobSchedulingContexts {
			jobSchedulingContextByExecutor, _ := repo.GetJobSchedulingContextByExecutor(jobId)
			qctx.SuccessfulJobSchedulingContexts[jobId] = jobSchedulingContextByExecutor[executorId]
		}
		for jobId := range qctx.UnsuccessfulJobSchedulingContexts {
			jobSchedulingContextByExecutor, _ := repo.GetJobSchedulingContextByExecutor(jobId)
			qctx.UnsuccessfulJobSchedulingContexts[jobId] = jobSchedulingContextByExecutor[executorId]
		}
	}
}

func (repo *SchedulingContextRepository) GetJobSchedulingContextByExecutor(jobId string) (JobSchedulingContextByExecutor, bool) {
	repo.rw.RLock()
	defer repo.rw.RUnlock()
	if v, ok := repo.jobSchedulingContextByExecutorByJobId.Get(jobId); ok {
		jobSchedulingContextByExecutor := v.(JobSchedulingContextByExecutor)
		return maps.Clone(jobSchedulingContextByExecutor), true
	} else {
		return nil, false
	}
}

// func (repo *SchedulingContextRepository) GetJobSchedulingReport(jobId string) (*JobSchedulingContext, bool) {
// 	if value, ok := repo.jobSchedulingContextByExecutorByJobId.Get(jobId); ok {
// 		report := value.(JobSchedulingContextByExecutor)
// 		return report, true
// 	} else {
// 		return nil, false
// 	}
// }

// func (report *QueueSchedulingContextContainer) String() string {
// 	var sb strings.Builder
// 	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
// 	fmt.Fprintf(w, "Queue:\t%s\n", report.QueueName)
// 	if report.MostRecentSuccessfulJobSchedulingContext != nil {
// 		fmt.Fprint(w, "Most recent successful scheduling attempt:\n")
// 		fmt.Fprint(w, indent.String("\t", report.MostRecentSuccessfulJobSchedulingContext.String()))
// 	} else {
// 		fmt.Fprint(w, "Most recent successful scheduling attempt:\tnone\n")
// 	}
// 	if report.MostRecentUnsuccessfulJobSchedulingContext != nil {
// 		fmt.Fprint(w, "Most recent unsuccessful scheduling attempt:\n")
// 		fmt.Fprint(w, indent.String("\t", report.MostRecentUnsuccessfulJobSchedulingContext.String()))
// 	} else {
// 		fmt.Fprint(w, "Most recent unsuccessful scheduling attempt:\n")
// 	}
// 	w.Flush()
// 	return sb.String()
// }
