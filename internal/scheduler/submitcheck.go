package scheduler

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/armadaproject/armada/internal/armada/configuration"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
)

type minimalExecutor struct {
	nodeDb     *NodeDb
	updateTime time.Time
}

type schedulingResult struct {
	isSchedulable bool
	reason        string
}

const maxJobSchedulingResults = 10000

type SubmitChecker struct {
	executorTimeout           time.Duration
	priorityClasses           map[string]configuration.PriorityClass
	gangIdAnnotation          string
	executorById              map[string]minimalExecutor
	priorities                []int32
	indexedResources          []string
	indexedTaints             []string
	indexedNodeLabels         []string
	executorRepository        database.ExecutorRepository
	clock                     clock.Clock
	mu                        sync.Mutex
	jobSchedulingResultsCache *lru.Cache
}

func NewSubmitChecker(
	executorTimeout time.Duration,
	schedulingConfig configuration.SchedulingConfig,
	executorRepository database.ExecutorRepository,
) *SubmitChecker {
	jobSchedulingResultsCache, err := lru.New(maxJobSchedulingResults)
	if err != nil {
		panic(errors.WithStack(err))
	}
	return &SubmitChecker{
		executorTimeout:           executorTimeout,
		priorityClasses:           schedulingConfig.Preemption.PriorityClasses,
		gangIdAnnotation:          configuration.GangIdAnnotation,
		executorById:              map[string]minimalExecutor{},
		priorities:                schedulingConfig.Preemption.AllowedPriorities(),
		indexedResources:          schedulingConfig.IndexedResources,
		indexedTaints:             schedulingConfig.IndexedTaints,
		indexedNodeLabels:         schedulingConfig.IndexedNodeLabels,
		executorRepository:        executorRepository,
		clock:                     clock.RealClock{},
		jobSchedulingResultsCache: jobSchedulingResultsCache,
	}
}

func (srv *SubmitChecker) Run(ctx context.Context) error {
	srv.updateExecutors(ctx)
	ticker := time.NewTicker(1 * time.Minute)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			srv.updateExecutors(ctx)
		}
	}
}

func (srv *SubmitChecker) updateExecutors(ctx context.Context) {
	executors, err := srv.executorRepository.GetExecutors(ctx)
	if err != nil {
		log.WithError(err).Error("Error fetching executors")
		return
	}
	for _, executor := range executors {
		nodeDb, err := srv.constructNodeDb(executor.Nodes)
		if err == nil {
			srv.mu.Lock()
			srv.executorById[executor.Id] = minimalExecutor{
				nodeDb:     nodeDb,
				updateTime: executor.LastUpdateTime,
			}
			srv.mu.Unlock()
			if err != nil {
				log.WithError(err).Errorf("Error constructing node db for executor %s", executor.Id)
			}
		} else {
			log.WithError(err).Warnf("Error clearing nodedb for executor %s", executor.Id)
		}

	}

	// Reset cache as the executors may have updated - changing what can be scheduled
	srv.jobSchedulingResultsCache.Purge()
}

func (srv *SubmitChecker) CheckApiJobs(jobs []*api.Job) (bool, string) {
	// First, check if all jobs can be scheduled individually.
	for i, job := range jobs {
		reqs := PodRequirementFromLegacySchedulerJob(job, srv.priorityClasses)
		schedulingResult := srv.getSchedulingResult([]*schedulerobjects.PodRequirements{reqs})
		if !schedulingResult.isSchedulable {
			return schedulingResult.isSchedulable, fmt.Sprintf("%d-th job unschedulable:\n%s", i, schedulingResult.reason)
		}
	}
	// Then, check if all gangs can be scheduled.
	for gangId, jobs := range GroupJobsByAnnotation(srv.gangIdAnnotation, jobs) {
		if gangId == "" {
			continue
		}
		reqs := PodRequirementsFromLegacySchedulerJobs(jobs, srv.priorityClasses)
		schedulingResult := srv.check(reqs)
		if !schedulingResult.isSchedulable {
			return schedulingResult.isSchedulable, fmt.Sprintf("gang %s is unschedulable:\n%s", gangId, schedulingResult.reason)
		}
	}
	return true, ""
}

func GroupJobsByAnnotation(annotation string, jobs []*api.Job) map[string][]*api.Job {
	rv := make(map[string][]*api.Job)
	for _, job := range jobs {
		if len(job.Annotations) == 0 {
			rv[""] = append(rv[""], job)
		} else {
			value := job.Annotations[annotation]
			rv[value] = append(rv[value], job)
		}
	}
	return rv
}

func (srv *SubmitChecker) getSchedulingResult(reqs []*schedulerobjects.PodRequirements) schedulingResult {
	overwriteAnnotations(reqs)
	reqsHash, err := protoutil.HashMany(reqs)
	if err != nil {
		return schedulingResult{isSchedulable: false, reason: err.Error()}
	}
	cachedResult, cacheExists := srv.jobSchedulingResultsCache.Get(reqsHash)
	result, castSuccess := cachedResult.(schedulingResult)

	if !cacheExists || !castSuccess {
		result = srv.check(reqs)
		srv.jobSchedulingResultsCache.Add(reqsHash, result)
	}

	return result
}

// overwriteAnnotations This sets all annotations to a constant value
// This is needed to reduce the cardinality of PodRequirements - so they hash more consistently
// To allow our caching to work effectively
func overwriteAnnotations(reqs []*schedulerobjects.PodRequirements) {
	for _, req := range reqs {
		for key := range req.GetAnnotations() {
			req.Annotations[key] = "submission-check"
		}
	}
}

// Check if a set of pods can be scheduled onto some cluster.
func (srv *SubmitChecker) check(reqs []*schedulerobjects.PodRequirements) schedulingResult {
	if len(reqs) == 0 {
		return schedulingResult{isSchedulable: true, reason: ""}
	}

	// Make a shallow copy to avoid holding the lock and
	// preventing updating NodeDbs while checking if jobs can be scheduled.
	srv.mu.Lock()
	executorById := maps.Clone(srv.executorById)
	srv.mu.Unlock()
	executorById = srv.filterStaleNodeDbs(executorById)
	if len(executorById) == 0 {
		return schedulingResult{isSchedulable: false, reason: "no executor clusters available"}
	}

	canSchedule := false
	var sb strings.Builder
	for id, executor := range executorById {
		nodeDb := executor.nodeDb
		txn := nodeDb.db.Txn(true)
		reports, ok, err := nodeDb.ScheduleManyWithTxn(txn, reqs)
		txn.Abort()

		sb.WriteString(id)
		if err != nil {
			sb.WriteString(err.Error())
			sb.WriteString("\n")
			continue
		}

		canSchedule = canSchedule || ok
		numSuccessfullyScheduled := 0
		for _, report := range reports {
			if report.Node != nil {
				numSuccessfullyScheduled++
			}
		}

		if len(reqs) == 1 {
			sb.WriteString(":\n")
			for _, report := range reports {
				sb.WriteString(report.String())
				sb.WriteString("\n")
			}
			sb.WriteString("---")
			sb.WriteString("\n")
		} else {
			sb.WriteString(":")
			sb.WriteString(fmt.Sprintf(" %d out of %d pods schedulable\n", numSuccessfullyScheduled, len(reqs)))
		}
	}
	return schedulingResult{isSchedulable: canSchedule, reason: sb.String()}
}

func (srv *SubmitChecker) filterStaleNodeDbs(executorsById map[string]minimalExecutor) map[string]minimalExecutor {
	rv := make(map[string]minimalExecutor)
	for id, executor := range executorsById {
		if srv.clock.Since(executor.updateTime) < srv.executorTimeout {
			rv[id] = executor
		}
	}
	return rv
}

func (srv *SubmitChecker) constructNodeDb(nodes []*schedulerobjects.Node) (*NodeDb, error) {
	// Nodes to be considered by the scheduler.
	// We just need to know if scheduling is possible;
	// no need to try to find a good fit.
	var maxExtraNodesToConsider uint = 0
	nodeDb, err := NewNodeDb(
		srv.priorityClasses,
		maxExtraNodesToConsider,
		srv.indexedResources,
		srv.indexedTaints,
		srv.indexedNodeLabels,
	)
	if err != nil {
		return nil, err
	}
	err = nodeDb.UpsertMany(nodes)
	if err != nil {
		return nil, err
	}
	err = nodeDb.ClearAllocated()
	if err != nil {
		return nil, err
	}
	return nodeDb, nil
}
