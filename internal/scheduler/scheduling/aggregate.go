package scheduling

import (
	"fmt"
	"sort"
	"strings"

	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/pkg/api"
)

// aggregateJobSchedulingInfo derives the per-pool scheduling information from
// the incrementally maintained JobDb aggregate instead of scanning every job.
// includeQueued and includeRunning select which parts to derive.
func aggregateJobSchedulingInfo(
	txn *jobdb.Txn,
	activeExecutorsSet map[string]bool,
	queues map[string]*api.Queue,
	currentPool string,
	awayAllocationPools []string,
	allPools []string,
	shortJobPenalty *ShortJobPenaltySnapshot,
	includeQueued bool,
	includeRunning bool,
) *jobSchedulingInfo {
	knownQueues := make(map[string]bool, len(queues))
	cordonedQueues := make(map[string]bool, len(queues))
	for name, queue := range queues {
		knownQueues[name] = true
		cordonedQueues[name] = queue.Cordoned
	}

	info := txn.CalculateSchedulingInfo(activeExecutorsSet, currentPool, awayAllocationPools, allPools, knownQueues, cordonedQueues, includeQueued, includeRunning)
	return &jobSchedulingInfo{
		jobsByExecutorId:                     info.JobsByExecutorId,
		jobsByPool:                           info.JobsByPool,
		demandByQueueAndPriorityClass:        info.DemandByQueueAndPriorityClass,
		allocatedByQueueAndPriorityClass:     info.AllocatedByQueueAndPriorityClass,
		awayAllocatedByQueueAndPriorityClass: info.AwayAllocatedByQueueAndPriorityClass,
		shortJobPenaltyByQueue:               shortJobPenalty.GetPenaltiesForPool(currentPool),
		inUsePriorityClasses:                 info.InUsePriorityClasses,
	}
}

// mergeJobSchedulingInfo combines the queued and running portions of the
// scheduling info into a single result.
func mergeJobSchedulingInfo(queued, running *jobSchedulingInfo, shortJobPenalty *ShortJobPenaltySnapshot, currentPool string) *jobSchedulingInfo {
	result := &jobSchedulingInfo{
		jobsByExecutorId:                     map[string][]*jobdb.Job{},
		jobsByPool:                           map[string][]*jobdb.Job{},
		demandByQueueAndPriorityClass:        map[string]map[string]internaltypes.ResourceList{},
		allocatedByQueueAndPriorityClass:     map[string]map[string]internaltypes.ResourceList{},
		awayAllocatedByQueueAndPriorityClass: map[string]map[string]internaltypes.ResourceList{},
		shortJobPenaltyByQueue:               shortJobPenalty.GetPenaltiesForPool(currentPool),
		inUsePriorityClasses:                 map[string]bool{},
	}
	if queued != nil {
		mergeResourceMaps(result.demandByQueueAndPriorityClass, queued.demandByQueueAndPriorityClass)
		for pc := range queued.inUsePriorityClasses {
			result.inUsePriorityClasses[pc] = true
		}
	}
	if running != nil {
		for executor, jobs := range running.jobsByExecutorId {
			result.jobsByExecutorId[executor] = append(result.jobsByExecutorId[executor], jobs...)
		}
		for pool, jobs := range running.jobsByPool {
			result.jobsByPool[pool] = append(result.jobsByPool[pool], jobs...)
		}
		mergeResourceMaps(result.demandByQueueAndPriorityClass, running.demandByQueueAndPriorityClass)
		mergeResourceMaps(result.allocatedByQueueAndPriorityClass, running.allocatedByQueueAndPriorityClass)
		mergeResourceMaps(result.awayAllocatedByQueueAndPriorityClass, running.awayAllocatedByQueueAndPriorityClass)
		for pc := range running.inUsePriorityClasses {
			result.inUsePriorityClasses[pc] = true
		}
	}
	return result
}

func mergeResourceMaps(dst, src map[string]map[string]internaltypes.ResourceList) {
	for queue, byPriorityClass := range src {
		dstByPriorityClass, ok := dst[queue]
		if !ok {
			dstByPriorityClass = map[string]internaltypes.ResourceList{}
			dst[queue] = dstByPriorityClass
		}
		for pc, rl := range byPriorityClass {
			dstByPriorityClass[pc] = dstByPriorityClass[pc].Add(rl)
		}
	}
}

// compareJobSchedulingInfo compares the scan-derived and aggregate-derived
// scheduling info and returns the names of the mismatching components together
// with a human-readable description of the differences. Both are empty if the
// two are equivalent. It is used by the canary mode to validate the aggregate
// against the established per-job calculation.
func compareJobSchedulingInfo(legacy, aggregate *jobSchedulingInfo) ([]string, string) {
	components := make([]string, 0)
	diffs := make([]string, 0)

	if !maps.Equal(legacy.inUsePriorityClasses, aggregate.inUsePriorityClasses) {
		components = append(components, "in_use_priority_classes")
		diffs = append(diffs, fmt.Sprintf("inUsePriorityClasses: scan=%v aggregate=%v",
			sortedKeys(legacy.inUsePriorityClasses), sortedKeys(aggregate.inUsePriorityClasses)))
	}
	if diff := compareResourceListMaps("demandByQueueAndPriorityClass", legacy.demandByQueueAndPriorityClass, aggregate.demandByQueueAndPriorityClass); diff != "" {
		components = append(components, "demand")
		diffs = append(diffs, diff)
	}
	if diff := compareResourceListMaps("allocatedByQueueAndPriorityClass", legacy.allocatedByQueueAndPriorityClass, aggregate.allocatedByQueueAndPriorityClass); diff != "" {
		components = append(components, "allocated")
		diffs = append(diffs, diff)
	}
	if diff := compareResourceListMaps("awayAllocatedByQueueAndPriorityClass", legacy.awayAllocatedByQueueAndPriorityClass, aggregate.awayAllocatedByQueueAndPriorityClass); diff != "" {
		components = append(components, "away_allocated")
		diffs = append(diffs, diff)
	}
	if diff := compareJobsById("jobsByPool", legacy.jobsByPool, aggregate.jobsByPool); diff != "" {
		components = append(components, "jobs_by_pool")
		diffs = append(diffs, diff)
	}
	if diff := compareJobsById("jobsByExecutorId", legacy.jobsByExecutorId, aggregate.jobsByExecutorId); diff != "" {
		components = append(components, "jobs_by_executor")
		diffs = append(diffs, diff)
	}
	return components, strings.Join(diffs, "; ")
}

func sortedKeys(m map[string]bool) []string {
	keys := maps.Keys(m)
	sort.Strings(keys)
	return keys
}

func compareResourceListMaps(name string, legacy, aggregate map[string]map[string]internaltypes.ResourceList) string {
	diffs := make([]string, 0)
	for _, queue := range unionKeys(legacy, aggregate) {
		for _, priorityClass := range unionResourcePriorityClasses(legacy[queue], aggregate[queue]) {
			legacyRL := legacy[queue][priorityClass]
			aggregateRL := aggregate[queue][priorityClass]
			legacyZero := legacyRL.AllZero()
			aggregateZero := aggregateRL.AllZero()
			if legacyZero && aggregateZero {
				continue
			}
			if legacyZero != aggregateZero || !legacyRL.Equal(aggregateRL) {
				diffs = append(diffs, fmt.Sprintf("%s[queue=%s,priorityClass=%s]: scan=%s aggregate=%s",
					name, queue, priorityClass, legacyRL.String(), aggregateRL.String()))
			}
		}
	}
	slices.Sort(diffs)
	return strings.Join(diffs, ", ")
}

func compareJobsById(name string, legacy, aggregate map[string][]*jobdb.Job) string {
	diffs := make([]string, 0)
	for _, key := range unionJobKeys(legacy, aggregate) {
		legacyIds := jobIdSet(legacy[key])
		aggregateIds := jobIdSet(aggregate[key])
		if !maps.Equal(legacyIds, aggregateIds) {
			diffs = append(diffs, fmt.Sprintf("%s[%s]: scan=%v aggregate=%v", name, key, sortedKeys(legacyIds), sortedKeys(aggregateIds)))
		}
	}
	slices.Sort(diffs)
	return strings.Join(diffs, ", ")
}

func unionKeys(a, b map[string]map[string]internaltypes.ResourceList) []string {
	keySet := make(map[string]bool, len(a)+len(b))
	for key := range a {
		keySet[key] = true
	}
	for key := range b {
		keySet[key] = true
	}
	keys := maps.Keys(keySet)
	sort.Strings(keys)
	return keys
}

func unionResourcePriorityClasses(a, b map[string]internaltypes.ResourceList) []string {
	keySet := make(map[string]bool, len(a)+len(b))
	for key := range a {
		keySet[key] = true
	}
	for key := range b {
		keySet[key] = true
	}
	keys := maps.Keys(keySet)
	sort.Strings(keys)
	return keys
}

func unionJobKeys(a, b map[string][]*jobdb.Job) []string {
	keySet := make(map[string]bool, len(a)+len(b))
	for key := range a {
		keySet[key] = true
	}
	for key := range b {
		keySet[key] = true
	}
	keys := maps.Keys(keySet)
	sort.Strings(keys)
	return keys
}

func jobIdSet(jobs []*jobdb.Job) map[string]bool {
	result := make(map[string]bool, len(jobs))
	for _, job := range jobs {
		result[job.Id()] = true
	}
	return result
}
