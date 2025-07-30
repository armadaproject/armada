package pricer

import (
	"fmt"
	"math"
	"sort"
	"time"

	"k8s.io/utils/set"

	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
)

type MinPriceNodeScheduler struct {
	jobDb jobdb.JobRepository
}

type NodeScheduler interface {
	Schedule(*context.JobSchedulingContext, *internaltypes.Node, set.Set[string]) (*NodeSchedulingResult, error)
}

func NewMinPriceNodeScheduler(jobDb jobdb.JobRepository) *MinPriceNodeScheduler {
	return &MinPriceNodeScheduler{
		jobDb: jobDb,
	}
}

// Schedule
// This function is responsible for determining if a job can be scheduled on a node, and the minimum price at which this can be achieved
// High level steps:
// - Determine all the jobs that can be preempted
// - Order these jobs by cost ascending
// - Attempt to schedule the new job on the node, preempting one job at a time until the job fits
// - Return NodeSchedulingResult, which is a summary of how the given job could be scheduled on the given node, including whether it's even possible
func (n *MinPriceNodeScheduler) Schedule(jctx *context.JobSchedulingContext, node *internaltypes.Node, protectedJobIds set.Set[string]) (*NodeSchedulingResult, error) {
	met, _, err := nodedb.StaticJobRequirementsMet(node, jctx)
	if err != nil {
		return nil, err
	}
	if !met {
		return &NodeSchedulingResult{
			jctx:      jctx,
			node:      node,
			resultId:  util.NewULID(),
			scheduled: false,
		}, nil
	}

	availableResource := node.AllocatableByPriority[internaltypes.EvictedPriority]
	if !jctx.Job.KubernetesResourceRequirements().Exceeds(availableResource) {
		return &NodeSchedulingResult{
			jctx:      jctx,
			node:      node,
			resultId:  util.NewULID(),
			scheduled: true,
		}, nil
	}

	allJobs, err := n.getJobDetails(node, protectedJobIds)
	if err != nil {
		return nil, err
	}
	sort.Sort(priceOrder(allJobs))

	scheduled := false
	maxPrice := float64(0)
	jobsToPreempt := []string{}
	for _, jobToEvict := range allJobs {
		availableResource = availableResource.Add(jobToEvict.resources)
		maxPrice = jobToEvict.cost
		jobsToPreempt = append(jobsToPreempt, jobToEvict.jobId)

		if !jctx.Job.KubernetesResourceRequirements().Exceeds(availableResource) {
			scheduled = true
			break
		}
	}

	if !scheduled {
		return &NodeSchedulingResult{
			scheduled: false,
			jctx:      jctx,
			node:      node,
			resultId:  util.NewULID(),
		}, nil
	}

	return &NodeSchedulingResult{
		scheduled:       true,
		jctx:            jctx,
		node:            node,
		price:           maxPrice,
		jobIdsToPreempt: jobsToPreempt,
		resultId:        util.NewULID(),
	}, nil
}

func (n *MinPriceNodeScheduler) getJobDetails(
	node *internaltypes.Node,
	excludedJobIds set.Set[string],
) ([]*jobDetails, error) {
	details := []*jobDetails{}
	start := time.Now()
	for jobId, jobResource := range node.AllocatedByJobId {
		if excludedJobIds.Has(jobId) {
			continue
		}
		job := n.jobDb.GetById(jobId)
		if job == nil {
			return nil, fmt.Errorf("job %s not found in jobDb", jobId)
		}
		if job.InTerminalState() {
			continue
		}
		queue := job.Queue()
		age := int64(0)
		if !job.Queued() {
			if job.LatestRun() == nil {
				return nil, fmt.Errorf("no job run found for scheduled job %s", jobId)
			}

			age = start.Sub(*job.LatestRun().LeaseTime()).Milliseconds()
		}

		cost := job.GetBidPrice(node.GetPool())
		runInfo := &jobDetails{
			queue:     queue,
			cost:      cost,
			resources: jobResource,
			jobId:     jobId,
			ageMillis: age,
		}

		details = append(details, runInfo)
	}

	return details, nil
}

func roundFloatHighPrecision(input float64) float64 {
	return math.Round(input*100000000) / 100000000
}
