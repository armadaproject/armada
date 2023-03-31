package scheduler

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"text/tabwriter"

	"github.com/gogo/protobuf/types"
	lru "github.com/hashicorp/golang-lru"
	"github.com/oklog/ulid"
	"github.com/openconfig/goyang/pkg/indent"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// SchedulingContextRepository stores scheduling contexts associated with recent scheduling attempts.
// On adding a context, a map is cloned, then mutated, and then swapped for the previous map using atomic pointers.
// Hence, reads concurrent with writes are safe and don't need locking.
// A mutex protects against concurrent writes.
type SchedulingContextRepository struct {
	// Maps executor id to *SchedulingContext.
	// The most recent attempt.
	mostRecentSchedulingContextByExecutorP atomic.Pointer[SchedulingContextByExecutor]
	// The most recent attempt where a non-zero amount of resources were scheduled.
	mostRecentSuccessfulSchedulingContextByExecutorP atomic.Pointer[SchedulingContextByExecutor]
	// Maps queue name to QueueSchedulingContextByExecutor.
	// The most recent attempt.
	mostRecentQueueSchedulingContextByExecutorByQueueP atomic.Pointer[map[string]QueueSchedulingContextByExecutor]
	// The most recent attempt where a non-zero amount of resources were scheduled.
	mostRecentSuccessfulQueueSchedulingContextByExecutorByQueueP atomic.Pointer[map[string]QueueSchedulingContextByExecutor]
	// Maps job id to JobSchedulingContextByExecutor.
	// We limit the number of job contexts to store to control memory usage.
	mostRecentJobSchedulingContextByExecutorByJobId *lru.Cache
	// Store all executor ids seen so far in a set.
	// Used to ensure all executors are included in reports.
	executorIds map[string]bool
	// All executors in sorted order.
	sortedExecutorIdsP atomic.Pointer[[]string]
	// Protects the fields in this struct from concurrent and dirty writes.
	mu sync.Mutex
}

type (
	SchedulingContextByExecutor      map[string]*SchedulingContext
	QueueSchedulingContextByExecutor map[string]*QueueSchedulingContext
	JobSchedulingContextByExecutor   map[string]*JobSchedulingContext
)

func NewSchedulingContextRepository(maxJobSchedulingContextsPerExecutor uint) (*SchedulingContextRepository, error) {
	jobSchedulingContextByExecutorByJobId, err := lru.New(int(maxJobSchedulingContextsPerExecutor))
	if err != nil {
		return nil, err
	}
	rv := &SchedulingContextRepository{
		mostRecentJobSchedulingContextByExecutorByJobId: jobSchedulingContextByExecutorByJobId,
		executorIds: make(map[string]bool),
	}
	mostRecentSchedulingContextByExecutor := make(SchedulingContextByExecutor)
	mostRecentSuccessfulSchedulingContextByExecutor := make(SchedulingContextByExecutor)
	mostRecentQueueSchedulingContextByExecutorByQueue := make(map[string]QueueSchedulingContextByExecutor)
	mostRecentSuccessfulQueueSchedulingContextByExecutorByQueue := make(map[string]QueueSchedulingContextByExecutor)
	sortedExecutorIds := make([]string, 0)
	rv.mostRecentSchedulingContextByExecutorP.Store(&mostRecentSchedulingContextByExecutor)
	rv.mostRecentSuccessfulSchedulingContextByExecutorP.Store(&mostRecentSuccessfulSchedulingContextByExecutor)
	rv.mostRecentQueueSchedulingContextByExecutorByQueueP.Store(&mostRecentQueueSchedulingContextByExecutorByQueue)
	rv.mostRecentSuccessfulQueueSchedulingContextByExecutorByQueueP.Store(&mostRecentSuccessfulQueueSchedulingContextByExecutorByQueue)
	rv.sortedExecutorIdsP.Store(&sortedExecutorIds)
	return rv, nil
}

// AddSchedulingContext adds a scheduling context to the repo.
// It also extracts the queue and job scheduling contexts it contains and stores those separately.
//
// It's safe to call this method concurrently with itself and with methods getting contexts from the repo.
// It's not safe to mutate contexts once they've been provided to this method.
//
// Job contexts are stored first, then queue contexts, and finally the scheduling context itself.
// This avoids having a stored scheduling (queue) context referring to a queue (job) context that isn't stored yet.
func (repo *SchedulingContextRepository) AddSchedulingContext(sctx *SchedulingContext) error {
	queueSchedulingContextByQueue, jobSchedulingContextByJobId := extractQueueAndJobContexts(sctx)
	repo.mu.Lock()
	defer repo.mu.Unlock()
	for _, jctx := range jobSchedulingContextByJobId {
		if err := repo.addJobSchedulingContext(jctx); err != nil {
			return err
		}
	}
	if err := repo.addQueueSchedulingContexts(maps.Values(queueSchedulingContextByQueue)); err != nil {
		return err
	}
	if err := repo.addSchedulingContext(sctx); err != nil {
		return err
	}
	if err := repo.addExecutorId(sctx.ExecutorId); err != nil {
		return err
	}
	return nil
}

// Should only be called from AddSchedulingContext to avoid concurrent and/or dirty writes.
func (repo *SchedulingContextRepository) addExecutorId(executorId string) error {
	n := len(repo.executorIds)
	repo.executorIds[executorId] = true
	if len(repo.executorIds) != n {
		sortedExecutorIds := maps.Keys(repo.executorIds)
		slices.Sort(sortedExecutorIds)
		repo.sortedExecutorIdsP.Store(&sortedExecutorIds)
	}
	return nil
}

// Should only be called from AddSchedulingContext to avoid dirty writes.
func (repo *SchedulingContextRepository) addSchedulingContext(sctx *SchedulingContext) error {
	mostRecentSchedulingContextByExecutor := *repo.mostRecentSchedulingContextByExecutorP.Load()
	mostRecentSchedulingContextByExecutor = maps.Clone(mostRecentSchedulingContextByExecutor)
	mostRecentSuccessfulSchedulingContextByExecutor := *repo.mostRecentSuccessfulSchedulingContextByExecutorP.Load()
	mostRecentSuccessfulSchedulingContextByExecutor = maps.Clone(mostRecentSuccessfulSchedulingContextByExecutor)
	mostRecentSchedulingContextByExecutor[sctx.ExecutorId] = sctx
	if !sctx.ScheduledResourcesByPriority.IsZero() {
		mostRecentSuccessfulSchedulingContextByExecutor[sctx.ExecutorId] = sctx
	}
	repo.mostRecentSchedulingContextByExecutorP.Store(&mostRecentSchedulingContextByExecutor)
	repo.mostRecentSuccessfulSchedulingContextByExecutorP.Store(&mostRecentSuccessfulSchedulingContextByExecutor)
	return nil
}

// Should only be called from AddSchedulingContext to avoid dirty writes.
func (repo *SchedulingContextRepository) addQueueSchedulingContexts(qctxs []*QueueSchedulingContext) error {
	mostRecentQueueSchedulingContextByExecutorByQueue := *repo.mostRecentQueueSchedulingContextByExecutorByQueueP.Load()
	mostRecentQueueSchedulingContextByExecutorByQueue = maps.Clone(mostRecentQueueSchedulingContextByExecutorByQueue)
	mostRecentSuccessfulQueueSchedulingContextByExecutorByQueue := *repo.mostRecentSuccessfulQueueSchedulingContextByExecutorByQueueP.Load()
	mostRecentSuccessfulQueueSchedulingContextByExecutorByQueue = maps.Clone(mostRecentSuccessfulQueueSchedulingContextByExecutorByQueue)
	for _, qctx := range qctxs {
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
		if previous := mostRecentQueueSchedulingContextByExecutorByQueue[qctx.Queue]; previous != nil {
			previous = maps.Clone(previous)
			previous[qctx.ExecutorId] = qctx
			mostRecentQueueSchedulingContextByExecutorByQueue[qctx.Queue] = previous
		} else {
			mostRecentQueueSchedulingContextByExecutorByQueue[qctx.Queue] = QueueSchedulingContextByExecutor{
				qctx.ExecutorId: qctx,
			}
		}
		if !qctx.ScheduledResourcesByPriority.IsZero() {
			if previous := mostRecentSuccessfulQueueSchedulingContextByExecutorByQueue[qctx.Queue]; previous != nil {
				previous = maps.Clone(previous)
				previous[qctx.ExecutorId] = qctx
				mostRecentSuccessfulQueueSchedulingContextByExecutorByQueue[qctx.Queue] = previous
			} else {
				mostRecentSuccessfulQueueSchedulingContextByExecutorByQueue[qctx.Queue] = QueueSchedulingContextByExecutor{
					qctx.ExecutorId: qctx,
				}
			}
		}
	}
	repo.mostRecentQueueSchedulingContextByExecutorByQueueP.Store(&mostRecentQueueSchedulingContextByExecutorByQueue)
	repo.mostRecentSuccessfulQueueSchedulingContextByExecutorByQueueP.Store(&mostRecentSuccessfulQueueSchedulingContextByExecutorByQueue)
	return nil
}

// Should only be called from AddSchedulingContext to avoid dirty writes.
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
	previous, ok, _ := repo.mostRecentJobSchedulingContextByExecutorByJobId.PeekOrAdd(
		jctx.JobId,
		JobSchedulingContextByExecutor{jctx.ExecutorId: jctx},
	)
	if ok {
		jobSchedulingContextByExecutor := previous.(JobSchedulingContextByExecutor)
		jobSchedulingContextByExecutor[jctx.ExecutorId] = jctx
		repo.mostRecentJobSchedulingContextByExecutorByJobId.Add(jctx.JobId, jobSchedulingContextByExecutor)
	}
	return nil
}

// extractQueueAndJobContexts extracts the job and queue scheduling contexts from the scheduling context,
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

// GetSchedulingReport is a gRPC endpoint for querying scheduler reports.
// TODO: Further separate this from internal contexts.
func (repo *SchedulingContextRepository) GetSchedulingReport(_ context.Context, _ *types.Empty) (*schedulerobjects.SchedulingReport, error) {
	return &schedulerobjects.SchedulingReport{
		Report: repo.getSchedulingReportString(),
	}, nil
}

func (repo *SchedulingContextRepository) getSchedulingReportString() string {
	sortedExecutorIds := repo.GetSortedExecutorIds()
	mostRecentSchedulingContextByExecutor := repo.GetMostRecentSchedulingContextByExecutor()
	mostRecentSuccessfulSchedulingContextByExecutor := repo.GetMostRecentSuccessfulSchedulingContextByExecutor()
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	for _, executorId := range sortedExecutorIds {
		fmt.Fprintf(w, "%s:\n", executorId)
		sctx := mostRecentSchedulingContextByExecutor[executorId]
		if sctx != nil {
			fmt.Fprint(w, indent.String("\t", "Most recent attempt:\n"))
			fmt.Fprint(w, indent.String("\t\t", sctx.String()))
		} else {
			fmt.Fprint(w, indent.String("\t", "Most recent attempt: none\n"))
		}
		sctx = mostRecentSuccessfulSchedulingContextByExecutor[executorId]
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

// GetQueueReport is a gRPC endpoint for querying queue reports.
// TODO: Further separate this from internal contexts.
func (repo *SchedulingContextRepository) GetQueueReport(_ context.Context, queue *schedulerobjects.Queue) (*schedulerobjects.QueueReport, error) {
	queueName := strings.TrimSpace(queue.Name)
	return &schedulerobjects.QueueReport{
		Report: repo.getQueueReportString(queueName),
	}, nil
}

func (repo *SchedulingContextRepository) getQueueReportString(queue string) string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	sortedExecutorIds := repo.GetSortedExecutorIds()
	mostRecentQueueSchedulingContextByExecutor, _ := repo.GetMostRecentQueueSchedulingContextByExecutor(queue)
	mostRecentSuccessfulQueueSchedulingContextByExecutor, _ := repo.GetMostRecentSuccessfulQueueSchedulingContextByExecutor(queue)
	for _, executorId := range sortedExecutorIds {
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

// GetJobReport is a gRPC endpoint for querying job reports.
// TODO: Further separate this from internal contexts.
func (repo *SchedulingContextRepository) GetJobReport(_ context.Context, jobId *schedulerobjects.JobId) (*schedulerobjects.JobReport, error) {
	key := strings.TrimSpace(jobId.Id)
	if _, err := ulid.Parse(key); err != nil {
		return nil, &armadaerrors.ErrInvalidArgument{
			Name:    "jobId",
			Value:   jobId.Id,
			Message: fmt.Sprintf("%s is not a valid jobId", jobId.Id),
		}
	}
	return &schedulerobjects.JobReport{
		Report: repo.getJobReportString(key),
	}, nil
}

func (repo *SchedulingContextRepository) getJobReportString(jobId string) string {
	sortedExecutorIds := repo.GetSortedExecutorIds()
	jobSchedulingContextByExecutor, _ := repo.GetMostRecentJobSchedulingContextByExecutor(jobId)
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	for _, executorId := range sortedExecutorIds {
		jctx := jobSchedulingContextByExecutor[executorId]
		if jctx != nil {
			fmt.Fprintf(w, "%s:\n", executorId)
			fmt.Fprint(w, indent.String("\t", jctx.String()))
		} else {
			fmt.Fprintf(w, "%s: no recent attempt\n", executorId)
		}
	}
	w.Flush()
	return sb.String()
}

func (repo *SchedulingContextRepository) GetMostRecentSchedulingContextByExecutor() SchedulingContextByExecutor {
	return *repo.mostRecentSchedulingContextByExecutorP.Load()
}

func (repo *SchedulingContextRepository) GetMostRecentSuccessfulSchedulingContextByExecutor() SchedulingContextByExecutor {
	return *repo.mostRecentSuccessfulSchedulingContextByExecutorP.Load()
}

func (repo *SchedulingContextRepository) GetMostRecentQueueSchedulingContextByExecutor(queue string) (QueueSchedulingContextByExecutor, bool) {
	mostRecentQueueSchedulingContextByExecutorByQueue := *repo.mostRecentQueueSchedulingContextByExecutorByQueueP.Load()
	mostRecentQueueSchedulingContextByExecutor, ok := mostRecentQueueSchedulingContextByExecutorByQueue[queue]
	return mostRecentQueueSchedulingContextByExecutor, ok
}

func (repo *SchedulingContextRepository) GetMostRecentSuccessfulQueueSchedulingContextByExecutor(queue string) (QueueSchedulingContextByExecutor, bool) {
	mostRecentSuccessfulQueueSchedulingContextByExecutorByQueue := *repo.mostRecentSuccessfulQueueSchedulingContextByExecutorByQueueP.Load()
	mostRecentSuccessfulQueueSchedulingContextByExecutor, ok := mostRecentSuccessfulQueueSchedulingContextByExecutorByQueue[queue]
	return mostRecentSuccessfulQueueSchedulingContextByExecutor, ok
}

func (repo *SchedulingContextRepository) GetMostRecentJobSchedulingContextByExecutor(jobId string) (JobSchedulingContextByExecutor, bool) {
	if v, ok := repo.mostRecentJobSchedulingContextByExecutorByJobId.Get(jobId); ok {
		jobSchedulingContextByExecutor := v.(JobSchedulingContextByExecutor)
		return jobSchedulingContextByExecutor, true
	} else {
		return nil, false
	}
}

func (repo *SchedulingContextRepository) GetSortedExecutorIds() []string {
	return *repo.sortedExecutorIdsP.Load()
}

func (m SchedulingContextByExecutor) String() string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	executorIds := maps.Keys(m)
	slices.Sort(executorIds)
	for _, executorId := range executorIds {
		sctx := m[executorId]
		fmt.Fprintf(w, "%s:\n", executorId)
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
		fmt.Fprintf(w, "%s:\n", executorId)
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
