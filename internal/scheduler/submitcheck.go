package scheduler

import (
	"context"
	"fmt"
	"k8s.io/apimachinery/pkg/util/clock"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
)

type minimalExecutor struct {
	nodeDb     *NodeDb
	updateTime time.Time
}

type SubmitChecker struct {
	executorTimeout    time.Duration
	priorityClasses    map[string]configuration.PriorityClass
	gangIdAnnotation   string
	executorById       map[string]minimalExecutor
	priorities         []int32
	indexedResources   []string
	indexedTaints      []string
	indexedNodeLabels  []string
	executorRepository database.ExecutorRepository
	clock              clock.Clock
	mu                 sync.Mutex
}

func NewSubmitChecker(
	executorTimeout time.Duration,
	schedulingConfig configuration.SchedulingConfig,
	executorRepository database.ExecutorRepository,
) *SubmitChecker {
	return &SubmitChecker{
		executorTimeout:    executorTimeout,
		priorityClasses:    schedulingConfig.Preemption.PriorityClasses,
		gangIdAnnotation:   schedulingConfig.GangIdAnnotation,
		executorById:       map[string]minimalExecutor{},
		priorities:         schedulingConfig.Preemption.AllowedPriorities(),
		indexedResources:   schedulingConfig.IndexedResources,
		indexedTaints:      schedulingConfig.IndexedTaints,
		indexedNodeLabels:  schedulingConfig.IndexedNodeLabels,
		executorRepository: executorRepository,
		clock:              clock.RealClock{},
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
}

func (srv *SubmitChecker) CheckApiJobs(jobs []*api.Job) (bool, string) {
	// First, check if all jobs can be scheduled individually.
	for i, job := range jobs {
		reqs := PodRequirementsFromLegacySchedulerJob(job, srv.priorityClasses)
		canSchedule, reason := srv.check([]*schedulerobjects.PodRequirements{reqs})
		if !canSchedule {
			return canSchedule, fmt.Sprintf("%d-th job unschedulable:\n%s", i, reason)
		}
	}
	// Then, check if all gangs can be scheduled.
	for gangId, jobs := range GroupJobsByAnnotation(srv.gangIdAnnotation, jobs) {
		if gangId == "" {
			continue
		}
		reqs := PodRequirementsFromLegacySchedulerJobs(jobs, srv.priorityClasses)
		canSchedule, reason := srv.check(reqs)
		if !canSchedule {
			return canSchedule, fmt.Sprintf("gang %s is unschedulable:\n%s", gangId, reason)
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

// Check if a set of pods can be scheduled onto some cluster.
func (srv *SubmitChecker) check(reqs []*schedulerobjects.PodRequirements) (bool, string) {
	if len(reqs) == 0 {
		return true, ""
	}

	// Make a shallow copy to avoid holding the lock and
	// preventing updating NodeDbs while checking if jobs can be scheduled.
	srv.mu.Lock()
	executorById := maps.Clone(srv.executorById)
	srv.mu.Unlock()
	executorById = srv.filterStaleNodeDbs(executorById)
	if len(executorById) == 0 {
		return false, "no executor clusters available"
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
	return canSchedule, sb.String()
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
	nodeDb, err := NewNodeDb(
		srv.priorities,
		srv.indexedResources,
		srv.indexedTaints,
		srv.indexedNodeLabels,
	)
	if err != nil {
		return nil, err
	}
	err = nodeDb.Upsert(nodes)
	if err != nil {
		return nil, err
	}
	err = nodeDb.ClearAllocated()
	if err != nil {
		return nil, err
	}
	return nodeDb, nil
}
