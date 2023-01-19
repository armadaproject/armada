package scheduler

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"golang.org/x/exp/maps"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
)

type SubmitChecker struct {
	executorTimeout  time.Duration
	priorityClasses  map[string]configuration.PriorityClass
	gangIdAnnotation string
	nodeDbByExecutor map[string]*NodeDb
	mu               sync.Mutex
}

func NewSubmitChecker(executorTimeout time.Duration, priorityClasses map[string]configuration.PriorityClass, gangIdAnnotation string) *SubmitChecker {
	return &SubmitChecker{
		executorTimeout:  executorTimeout,
		priorityClasses:  priorityClasses,
		gangIdAnnotation: gangIdAnnotation,
	}
}

// RegisterNodeDb adds a NodeDb to use when checking if a pod can be scheduled.
// To only check static scheduling requirements, set NodeDb.CheckOnlyStaticRequirements = true
// before registering it.
func (srv *SubmitChecker) RegisterNodeDb(executor string, nodeDb *NodeDb) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.nodeDbByExecutor == nil {
		srv.nodeDbByExecutor = make(map[string]*NodeDb)
	}
	srv.nodeDbByExecutor[executor] = nodeDb
}

func (srv *SubmitChecker) CheckApiJobs(jobs []*api.Job) (bool, string) {
	// // First, check if all jobs can be scheduled individually.
	// // TODO: Disabled for now; the SubmitChecker would reject all jobs until populated.
	// for i, job := range jobs {
	// 	reqs := PodRequirementsFromJob(srv.priorityClasses, job)
	// 	canSchedule, reason := srv.Check(reqs)
	// 	if !canSchedule {
	// 		return canSchedule, fmt.Sprintf("%d-th job unschedulable:\n%s", i, reason)
	// 	}
	// }
	// Then, check if all gangs can be scheduled.
	for gangId, jobs := range groupJobsByAnnotation(srv.gangIdAnnotation, jobs) {
		if gangId == "" {
			continue
		}
		reqs := PodRequirementsFromLegacySchedulerJobs(jobs, srv.priorityClasses)
		// TODO: Fix
		// if err != nil {
		// 	return false, fmt.Sprintf("error encountered extracting scheduling requirements")
		// }
		canSchedule, reason := srv.Check(reqs)
		if !canSchedule {
			return canSchedule, fmt.Sprintf("gang %s is unschedulable:\n%s", gangId, reason)
		}
	}
	return true, ""
}

func groupJobsByAnnotation(annotation string, jobs []*api.Job) map[string][]*api.Job {
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
func (srv *SubmitChecker) Check(reqs []*schedulerobjects.PodRequirements) (bool, string) {
	if len(reqs) == 0 {
		return true, ""
	}

	// Make a shallow copy to avoid holding the lock and
	// preventing registering new NodeDbs while checking if jobs can be scheduled.
	srv.mu.Lock()
	nodeDbByExecutor := maps.Clone(srv.nodeDbByExecutor)
	srv.mu.Unlock()
	nodeDbByExecutor = srv.filterStaleNodeDbs(nodeDbByExecutor)
	if len(nodeDbByExecutor) == 0 {
		return false, "no executor clusters available"
	}

	canSchedule := false
	var sb strings.Builder
	for executor, nodeDb := range nodeDbByExecutor {
		txn := nodeDb.db.Txn(true)
		reports, ok, err := nodeDb.ScheduleManyWithTxn(txn, reqs)
		txn.Abort()

		sb.WriteString(executor)
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

func (srv *SubmitChecker) filterStaleNodeDbs(nodeDbByExecutor map[string]*NodeDb) map[string]*NodeDb {
	rv := make(map[string]*NodeDb)
	for executor, nodeDb := range nodeDbByExecutor {
		if time.Since(nodeDb.TimeOfMostRecentUpsert()) < srv.executorTimeout {
			rv[executor] = nodeDb
		}
	}
	return rv
}
