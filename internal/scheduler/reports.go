package scheduler

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"text/tabwriter"

	lru "github.com/hashicorp/golang-lru"
	"github.com/oklog/ulid"
	"github.com/openconfig/goyang/pkg/indent"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// SchedulingContextRepository stores scheduling contexts associated with recent scheduling attempts.
// On adding a context, a map is cloned, then mutated, and then swapped for the previous map using atomic pointers.
// Hence, reads concurrent with writes are safe and don't need locking.
// A mutex protects against concurrent writes.
type SchedulingContextRepository struct {
	// Maps executor id to *schedulercontext.SchedulingContext.
	// The most recent attempt.
	mostRecentByExecutor atomic.Pointer[SchedulingContextByExecutor]
	// The most recent attempt where a non-zero amount of resources were scheduled.
	mostRecentSuccessfulByExecutor atomic.Pointer[SchedulingContextByExecutor]
	// The most recent attempt that preempted at least one job.
	mostRecentPreemptingByExecutor atomic.Pointer[SchedulingContextByExecutor]

	// Maps queue name to SchedulingContextByExecutor.
	// The most recent attempt.
	mostRecentByExecutorByQueue atomic.Pointer[map[string]SchedulingContextByExecutor]
	// The most recent attempt where a non-zero amount of resources were scheduled.
	mostRecentSuccessfulByExecutorByQueue atomic.Pointer[map[string]SchedulingContextByExecutor]
	// The most recent attempt that preempted at least one job belonging to this queue.
	mostRecentPreemptingByExecutorByQueue atomic.Pointer[map[string]SchedulingContextByExecutor]

	// Maps job ID to SchedulingContextByExecutor.
	// We limit the number of job contexts to store to control memory usage.
	mostRecentByExecutorByJobId *lru.Cache

	// Store all executor ids seen so far in a set.
	// Used to ensure all executors are included in reports.
	executorIds map[string]bool
	// All executors in sorted order.
	sortedExecutorIds atomic.Pointer[[]string]

	// Protects the fields in this struct from concurrent and dirty writes.
	mu sync.Mutex
}

type SchedulingContextByExecutor map[string]*schedulercontext.SchedulingContext

func NewSchedulingContextRepository(jobCacheSize uint) (*SchedulingContextRepository, error) {
	mostRecentByExecutorByJobId, err := lru.New(int(jobCacheSize))
	if err != nil {
		return nil, err
	}
	rv := &SchedulingContextRepository{
		mostRecentByExecutorByJobId: mostRecentByExecutorByJobId,
		executorIds:                 make(map[string]bool),
	}

	mostRecentByExecutor := make(SchedulingContextByExecutor)
	mostRecentSuccessfulByExecutor := make(SchedulingContextByExecutor)
	mostRecentPreemptingByExecutor := make(SchedulingContextByExecutor)

	mostRecentByExecutorByQueue := make(map[string]SchedulingContextByExecutor)
	mostRecentSuccessfulByExecutorByQueue := make(map[string]SchedulingContextByExecutor)
	mostRecentPreemptingByExecutorByQueue := make(map[string]SchedulingContextByExecutor)

	sortedExecutorIds := make([]string, 0)

	rv.mostRecentByExecutor.Store(&mostRecentByExecutor)
	rv.mostRecentSuccessfulByExecutor.Store(&mostRecentSuccessfulByExecutor)
	rv.mostRecentPreemptingByExecutor.Store(&mostRecentPreemptingByExecutor)

	rv.mostRecentByExecutorByQueue.Store(&mostRecentByExecutorByQueue)
	rv.mostRecentSuccessfulByExecutorByQueue.Store(&mostRecentSuccessfulByExecutorByQueue)
	rv.mostRecentPreemptingByExecutorByQueue.Store(&mostRecentPreemptingByExecutorByQueue)

	rv.sortedExecutorIds.Store(&sortedExecutorIds)

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
func (repo *SchedulingContextRepository) AddSchedulingContext(sctx *schedulercontext.SchedulingContext) error {
	qctxs, jctxs := extractQueueAndJobContexts(sctx)
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if err := repo.addJobSchedulingContexts(sctx, jctxs); err != nil {
		return err
	}
	if err := repo.addQueueSchedulingContexts(sctx, qctxs); err != nil {
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
		repo.sortedExecutorIds.Store(&sortedExecutorIds)
	}
	return nil
}

// Should only be called from AddSchedulingContext to avoid dirty writes.
func (repo *SchedulingContextRepository) addSchedulingContext(sctx *schedulercontext.SchedulingContext) error {
	mostRecentByExecutor := *repo.mostRecentByExecutor.Load()
	mostRecentByExecutor = maps.Clone(mostRecentByExecutor)
	mostRecentByExecutor[sctx.ExecutorId] = sctx

	mostRecentSuccessfulByExecutor := *repo.mostRecentSuccessfulByExecutor.Load()
	mostRecentSuccessfulByExecutor = maps.Clone(mostRecentSuccessfulByExecutor)
	if !sctx.ScheduledResourcesByPriority.IsZero() {
		mostRecentSuccessfulByExecutor[sctx.ExecutorId] = sctx
	}

	mostRecentPreemptingByExecutor := *repo.mostRecentPreemptingByExecutor.Load()
	mostRecentPreemptingByExecutor = maps.Clone(mostRecentPreemptingByExecutor)
	if !sctx.EvictedResourcesByPriority.IsZero() {
		mostRecentPreemptingByExecutor[sctx.ExecutorId] = sctx
	}

	repo.mostRecentByExecutor.Store(&mostRecentByExecutor)
	repo.mostRecentSuccessfulByExecutor.Store(&mostRecentSuccessfulByExecutor)
	repo.mostRecentPreemptingByExecutor.Store(&mostRecentPreemptingByExecutor)

	return nil
}

// Should only be called from AddSchedulingContext to avoid dirty writes.
func (repo *SchedulingContextRepository) addQueueSchedulingContexts(sctx *schedulercontext.SchedulingContext, qctxs []*schedulercontext.QueueSchedulingContext) error {
	executorId := sctx.ExecutorId
	if executorId == "" {
		return errors.WithStack(
			&armadaerrors.ErrInvalidArgument{
				Name:    "ExecutorId",
				Value:   "",
				Message: "received empty executorId",
			},
		)
	}

	mostRecentByExecutorByQueue := maps.Clone(*repo.mostRecentByExecutorByQueue.Load())
	mostRecentSuccessfulByExecutorByQueue := maps.Clone(*repo.mostRecentSuccessfulByExecutorByQueue.Load())
	mostRecentPreemptingByExecutorByQueue := maps.Clone(*repo.mostRecentPreemptingByExecutorByQueue.Load())

	for _, qctx := range qctxs {
		queue := qctx.Queue
		if queue == "" {
			return errors.WithStack(
				&armadaerrors.ErrInvalidArgument{
					Name:    "Queue",
					Value:   "",
					Message: "received empty queue name",
				},
			)
		}

		if previous := mostRecentByExecutorByQueue[queue]; previous != nil {
			previous = maps.Clone(previous)
			previous[executorId] = sctx
			mostRecentByExecutorByQueue[queue] = previous
		} else {
			mostRecentByExecutorByQueue[queue] = SchedulingContextByExecutor{executorId: sctx}
		}

		if !qctx.ScheduledResourcesByPriority.IsZero() {
			if previous := mostRecentSuccessfulByExecutorByQueue[queue]; previous != nil {
				previous = maps.Clone(previous)
				previous[executorId] = sctx
				mostRecentSuccessfulByExecutorByQueue[queue] = previous
			} else {
				mostRecentSuccessfulByExecutorByQueue[queue] = SchedulingContextByExecutor{executorId: sctx}
			}
		}

		if !qctx.EvictedResourcesByPriority.IsZero() {
			if previous := mostRecentPreemptingByExecutorByQueue[queue]; previous != nil {
				previous = maps.Clone(previous)
				previous[executorId] = sctx
				mostRecentPreemptingByExecutorByQueue[queue] = previous
			} else {
				mostRecentPreemptingByExecutorByQueue[queue] = SchedulingContextByExecutor{executorId: sctx}
			}
		}
	}

	repo.mostRecentByExecutorByQueue.Store(&mostRecentByExecutorByQueue)
	repo.mostRecentSuccessfulByExecutorByQueue.Store(&mostRecentSuccessfulByExecutorByQueue)
	repo.mostRecentPreemptingByExecutorByQueue.Store(&mostRecentPreemptingByExecutorByQueue)

	return nil
}

// Should only be called from AddSchedulingContext to avoid dirty writes.
func (repo *SchedulingContextRepository) addJobSchedulingContexts(sctx *schedulercontext.SchedulingContext, jctxs []*schedulercontext.JobSchedulingContext) error {
	executorId := sctx.ExecutorId
	if executorId == "" {
		return errors.WithStack(
			&armadaerrors.ErrInvalidArgument{
				Name:    "ExecutorId",
				Value:   "",
				Message: "received empty executorId",
			},
		)
	}
	for _, jctx := range jctxs {
		jobId := jctx.JobId
		if jobId == "" {
			return errors.WithStack(
				&armadaerrors.ErrInvalidArgument{
					Name:    "JobId",
					Value:   "",
					Message: "received empty jobId",
				},
			)
		}
		if previous, ok, _ := repo.mostRecentByExecutorByJobId.PeekOrAdd(jobId, SchedulingContextByExecutor{executorId: sctx}); ok {
			byExecutor := previous.(SchedulingContextByExecutor)
			byExecutor[executorId] = sctx
			repo.mostRecentByExecutorByJobId.Add(jobId, byExecutor)
		}
	}
	return nil
}

// extractQueueAndJobContexts extracts the job and queue scheduling contexts from the scheduling context,
// and returns those separately.
func extractQueueAndJobContexts(sctx *schedulercontext.SchedulingContext) ([]*schedulercontext.QueueSchedulingContext, []*schedulercontext.JobSchedulingContext) {
	qctxs := make([]*schedulercontext.QueueSchedulingContext, 0)
	jctxs := make([]*schedulercontext.JobSchedulingContext, 0)
	for _, qctx := range sctx.QueueSchedulingContexts {
		qctxs = append(qctxs, qctx)
		for _, jctx := range qctx.SuccessfulJobSchedulingContexts {
			jctxs = append(jctxs, jctx)
		}
		for _, jctx := range qctx.UnsuccessfulJobSchedulingContexts {
			jctxs = append(jctxs, jctx)
		}
	}
	return qctxs, jctxs
}

func (repo *SchedulingContextRepository) getSchedulingReportStringForQueue(queue string, verbosity int32) string {
	mostRecentByExecutor, _ := repo.GetMostRecentSchedulingContextByExecutorForQueue(queue)
	mostRecentSuccessfulByExecutor, _ := repo.GetMostRecentSuccessfulSchedulingContextByExecutorForQueue(queue)
	mostRecentPreemptingByExecutor, _ := repo.GetMostRecentPreemptingSchedulingContextByExecutorForQueue(queue)
	sr := schedulingReport{
		mostRecentByExecutor:           mostRecentByExecutor,
		mostRecentSuccessfulByExecutor: mostRecentSuccessfulByExecutor,
		mostRecentPreemptingByExecutor: mostRecentPreemptingByExecutor,

		sortedExecutorIds: repo.GetSortedExecutorIds(),
	}
	return sr.ReportString(verbosity)
}

func (repo *SchedulingContextRepository) getSchedulingReportStringForJob(jobId string, verbosity int32) string {
	mostRecentByExecutor, ok := repo.GetMostRecentSchedulingContextByExecutorForJob(jobId)
	if !ok {
		mostRecentByExecutor = make(SchedulingContextByExecutor)
	}
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	for _, executorId := range repo.GetSortedExecutorIds() {
		fmt.Fprintf(w, "%s:\n", executorId)
		sctx := mostRecentByExecutor[executorId]
		if sctx != nil {
			fmt.Fprint(w, indent.String("\t", "Most recent attempt:\n"))
			fmt.Fprint(w, indent.String("\t\t", sctx.ReportString(verbosity)))
		} else {
			fmt.Fprint(w, indent.String("\t", "Most recent attempt: none\n"))
		}
	}
	w.Flush()
	return sb.String()
}

func (repo *SchedulingContextRepository) getSchedulingReportString(verbosity int32) string {
	sr := schedulingReport{
		mostRecentByExecutor:           repo.GetMostRecentSchedulingContextByExecutor(),
		mostRecentSuccessfulByExecutor: repo.GetMostRecentSuccessfulSchedulingContextByExecutor(),
		mostRecentPreemptingByExecutor: repo.GetMostRecentPreemptingSchedulingContextByExecutor(),

		sortedExecutorIds: repo.GetSortedExecutorIds(),
	}
	return sr.ReportString(verbosity)
}

// GetSchedulingReport is a gRPC endpoint for querying scheduler reports.
// TODO: Further separate this from internal contexts.
func (repo *SchedulingContextRepository) GetSchedulingReport(_ context.Context, request *schedulerobjects.SchedulingReportRequest) (*schedulerobjects.SchedulingReport, error) {
	var report string
	verbosity := request.GetVerbosity()
	switch filter := request.GetFilter().(type) {
	case *schedulerobjects.SchedulingReportRequest_MostRecentForQueue:
		queueName := strings.TrimSpace(filter.MostRecentForQueue.GetQueueName())
		report = repo.getSchedulingReportStringForQueue(queueName, verbosity)
	case *schedulerobjects.SchedulingReportRequest_MostRecentForJob:
		jobId := strings.TrimSpace(filter.MostRecentForJob.GetJobId())
		report = repo.getSchedulingReportStringForJob(jobId, verbosity)
	default:
		report = repo.getSchedulingReportString(verbosity)
	}
	return &schedulerobjects.SchedulingReport{Report: report}, nil
}

type schedulingReport struct {
	mostRecentByExecutor           SchedulingContextByExecutor
	mostRecentSuccessfulByExecutor SchedulingContextByExecutor
	mostRecentPreemptingByExecutor SchedulingContextByExecutor

	sortedExecutorIds []string
}

func (sr schedulingReport) ReportString(verbosity int32) string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	for _, executorId := range sr.sortedExecutorIds {
		fmt.Fprintf(w, "%s:\n", executorId)
		sctx := sr.mostRecentByExecutor[executorId]
		if sctx != nil {
			fmt.Fprint(w, indent.String("\t", "Most recent attempt:\n"))
			fmt.Fprint(w, indent.String("\t\t", sctx.ReportString(verbosity)))
		} else {
			fmt.Fprint(w, indent.String("\t", "Most recent attempt: none\n"))
		}
		sctx = sr.mostRecentSuccessfulByExecutor[executorId]
		if sctx != nil {
			fmt.Fprint(w, indent.String("\t", "Most recent successful attempt:\n"))
			fmt.Fprint(w, indent.String("\t\t", sctx.ReportString(verbosity)))
		} else {
			fmt.Fprint(w, indent.String("\t", "Most recent successful attempt: none\n"))
		}
		sctx = sr.mostRecentPreemptingByExecutor[executorId]
		if sctx != nil {
			fmt.Fprint(w, indent.String("\t", "Most recent preempting attempt:\n"))
			fmt.Fprint(w, indent.String("\t\t", sctx.ReportString(verbosity)))
		} else {
			fmt.Fprint(w, indent.String("\t", "Most recent preempting attempt: none\n"))
		}
	}
	w.Flush()
	return sb.String()
}

// GetQueueReport is a gRPC endpoint for querying queue reports.
// TODO: Further separate this from internal contexts.
func (repo *SchedulingContextRepository) GetQueueReport(_ context.Context, request *schedulerobjects.QueueReportRequest) (*schedulerobjects.QueueReport, error) {
	queueName := strings.TrimSpace(request.GetQueueName())
	verbosity := request.GetVerbosity()
	return &schedulerobjects.QueueReport{
		Report: repo.getQueueReportString(queueName, verbosity),
	}, nil
}

func (repo *SchedulingContextRepository) getQueueReportString(queue string, verbosity int32) string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	sortedExecutorIds := repo.GetSortedExecutorIds()
	mostRecentByExecutor, ok := repo.GetMostRecentSchedulingContextByExecutorForQueue(queue)
	if !ok {
		mostRecentByExecutor = make(SchedulingContextByExecutor)
	}
	mostRecentSuccessfulByExecutor, ok := repo.GetMostRecentSuccessfulSchedulingContextByExecutorForQueue(queue)
	if !ok {
		mostRecentSuccessfulByExecutor = make(SchedulingContextByExecutor)
	}
	mostRecentPreemptingByExecutor, ok := repo.GetMostRecentPreemptingSchedulingContextByExecutorForQueue(queue)
	if !ok {
		mostRecentPreemptingByExecutor = make(SchedulingContextByExecutor)
	}
	for _, executorId := range sortedExecutorIds {
		fmt.Fprintf(w, "%s:\n", executorId)
		qctx := mostRecentByExecutor[executorId]
		if qctx != nil {
			fmt.Fprint(w, indent.String("\t", "Most recent attempt:\n"))
			fmt.Fprint(w, indent.String("\t\t", qctx.ReportString(verbosity)))
		} else {
			fmt.Fprint(w, indent.String("\t", "Most recent attempt: none\n"))
		}
		qctx = mostRecentSuccessfulByExecutor[executorId]
		if qctx != nil {
			fmt.Fprint(w, indent.String("\t", "Most recent successful attempt:\n"))
			fmt.Fprint(w, indent.String("\t\t", qctx.ReportString(verbosity)))
		} else {
			fmt.Fprint(w, indent.String("\t", "Most recent successful attempt: none\n"))
		}
		qctx = mostRecentPreemptingByExecutor[executorId]
		if qctx != nil {
			fmt.Fprint(w, indent.String("\t", "Most recent preempting attempt:\n"))
			fmt.Fprint(w, indent.String("\t\t", qctx.ReportString(verbosity)))
		} else {
			fmt.Fprint(w, indent.String("\t", "Most recent preempting attempt: none\n"))
		}
	}
	w.Flush()
	return sb.String()
}

// GetJobReport is a gRPC endpoint for querying job reports.
// TODO: Further separate this from internal contexts.
func (repo *SchedulingContextRepository) GetJobReport(_ context.Context, request *schedulerobjects.JobReportRequest) (*schedulerobjects.JobReport, error) {
	jobId := strings.TrimSpace(request.GetJobId())
	if _, err := ulid.Parse(jobId); err != nil {
		return nil, &armadaerrors.ErrInvalidArgument{
			Name:    "jobId",
			Value:   request.GetJobId(),
			Message: fmt.Sprintf("%s is not a valid jobId", request.GetJobId()),
		}
	}
	return &schedulerobjects.JobReport{
		Report: repo.getJobReportString(jobId),
	}, nil
}

func (repo *SchedulingContextRepository) getJobReportString(jobId string) string {
	byExecutor, ok := repo.GetMostRecentSchedulingContextByExecutorForJob(jobId)
	if !ok {
		byExecutor = make(SchedulingContextByExecutor)
	}
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	for _, executorId := range repo.GetSortedExecutorIds() {
		sctx := byExecutor[executorId]
		var jctx *schedulercontext.JobSchedulingContext
		if sctx != nil {
			for _, qctx := range sctx.QueueSchedulingContexts {
				if jctx, _ = qctx.SuccessfulJobSchedulingContexts[jobId]; jctx != nil {
					break
				}
				if jctx, _ = qctx.UnsuccessfulJobSchedulingContexts[jobId]; jctx != nil {
					break
				}
			}
		}
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
	return *repo.mostRecentByExecutor.Load()
}

func (repo *SchedulingContextRepository) GetMostRecentSuccessfulSchedulingContextByExecutor() SchedulingContextByExecutor {
	return *repo.mostRecentSuccessfulByExecutor.Load()
}

func (repo *SchedulingContextRepository) GetMostRecentPreemptingSchedulingContextByExecutor() SchedulingContextByExecutor {
	return *repo.mostRecentPreemptingByExecutor.Load()
}

func (repo *SchedulingContextRepository) GetMostRecentSchedulingContextByExecutorForQueue(queue string) (SchedulingContextByExecutor, bool) {
	mostRecentByExecutorByQueue := *repo.mostRecentByExecutorByQueue.Load()
	mostRecentByExecutor, ok := mostRecentByExecutorByQueue[queue]
	return mostRecentByExecutor, ok
}

func (repo *SchedulingContextRepository) GetMostRecentSuccessfulSchedulingContextByExecutorForQueue(queue string) (SchedulingContextByExecutor, bool) {
	mostRecentSuccessfulByExecutorByQueue := *repo.mostRecentSuccessfulByExecutorByQueue.Load()
	mostRecentSuccessfulByExecutor, ok := mostRecentSuccessfulByExecutorByQueue[queue]
	return mostRecentSuccessfulByExecutor, ok
}

func (repo *SchedulingContextRepository) GetMostRecentPreemptingSchedulingContextByExecutorForQueue(queue string) (SchedulingContextByExecutor, bool) {
	mostRecentPreemptingByExecutorByQueue := *repo.mostRecentPreemptingByExecutorByQueue.Load()
	mostRecentPreemptingByExecutor, ok := mostRecentPreemptingByExecutorByQueue[queue]
	return mostRecentPreemptingByExecutor, ok
}

func (repo *SchedulingContextRepository) GetMostRecentSchedulingContextByExecutorForJob(jobId string) (SchedulingContextByExecutor, bool) {
	if value, ok := repo.mostRecentByExecutorByJobId.Get(jobId); ok {
		return value.(SchedulingContextByExecutor), true
	}
	return nil, false
}

func (repo *SchedulingContextRepository) GetSortedExecutorIds() []string {
	return *repo.sortedExecutorIds.Load()
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
