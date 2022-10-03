package scheduling

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"

	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"

	"github.com/pkg/errors"

	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/pkg/api"
)

const leaseDeadlineTolerance = time.Second * 3

type JobQueue interface {
	PeekClusterQueue(clusterId, queue string, limit int64) ([]*api.Job, error)
	TryLeaseJobs(clusterId string, queue string, jobs []*api.Job) ([]*api.Job, error)
}

type leaseContext struct {
	schedulingConfig *configuration.SchedulingConfig
	queue            JobQueue
	onJobsLeased     func([]*api.Job)

	clusterId string

	queueSchedulingInfo map[*api.Queue]*QueueSchedulingInfo
	resourceScarcity    map[string]float64
	priorities          map[*api.Queue]QueuePriorityInfo

	// Map from queue name to a report with leased jobs for that queue.
	// Computed from the lease request.
	leasedByQueue  map[string]*api.QueueLeasedReport
	nodeResources  []*nodeTypeAllocation
	minimumJobSize map[string]resource.Quantity

	queueCache map[string][]*api.Job

	schedulingReportByQueueName map[string]*queueSchedulingRoundReport

	// Contains info about the scheduling decisions made for a given queue.
	// One such report is created every time a queue is selected.
	queueSchedulingRoundReports []*queueSchedulingRoundReport
	// Overall report per queue.
	queueSchedulingReportByQueue map[string]*queueSchedulingReport
}

// queueSchedulingRoundReport contains information about jobs scheduled in one round of the scheduler.
type queueSchedulingRoundReport struct {
	// When this report was created.
	created time.Time
	// Name of the queue this report refers to.
	queueName string
	// Probability of selecing this queue in the first scheduling round.
	queueSelectionProbability float64
	// Total amount of resources that can be consumed by jobs from this queue,
	// as computed in the first round this queue was considered.
	resourceShare common.ComputeResourcesFloat
	// Amount of resources from this queue that are preemptible.
	preemptibleResources common.ComputeResourcesFloat
	// Total number of jobs leased in this round for this queue.
	numJobsLeased int
	// Total resources consumed by jobs leased in this round.
	leasedResources common.ComputeResourcesFloat
	// Maps job ids to the reason for why those jobs were not scheduled.
	unschedulableReasonByJobId map[string]string
}

// queueSchedulingReport contains information about jobs scheduled in one invocation of the scheduler.
// Each invocation consists of several rounds.
type queueSchedulingReport struct {
	// When this report was created.
	created time.Time
	// Name of the queue this report refers to.
	queueName string
	// Probability of selecing this queue in the first scheduling round.
	initialQueueSelectionProbability float64
	// Total amount of resources that can be consumed by jobs from this queue,
	// as computed in the first round this queue was considered.
	initialResourceShare common.ComputeResourcesFloat
	// Amount of resources from this queue that are preemptible.
	initialPreemptibleResources common.ComputeResourcesFloat
	// Number of times this queue was selected by the scheduler.
	numTimesSelected int
	// Total number of jobs leased for this queue.
	numJobsLeased int
	// Total resources consumed by jobs leased for this queue.
	leasedResources common.ComputeResourcesFloat
	// Maps job ids to the reason for why those jobs were not scheduled.
	unschedulableReasonByJobId map[string]string
}

func (report *queueSchedulingReport) asFields() map[string]interface{} {
	rv := make(map[string]interface{})
	rv["queue"] = report.queueName
	for t, q := range report.initialResourceShare {
		rv[fmt.Sprintf("%s_max", t)] = fmt.Sprintf("%f", q)
	}
	for t, q := range report.initialPreemptibleResources {
		rv[fmt.Sprintf("%s_preemptible", t)] = fmt.Sprintf("%f", q)
	}
	rv["numTimesSelected"] = fmt.Sprintf("%d", report.numTimesSelected)
	rv["numJobsLeased"] = fmt.Sprintf("%d", report.numJobsLeased)
	for t, q := range report.leasedResources {
		rv[fmt.Sprintf("%s_leased", t)] = fmt.Sprintf("%f", q)
	}
	rv["numUnschedulableJobs"] = fmt.Sprintf("%d", len(report.unschedulableReasonByJobId))
	return rv
}

func (c *leaseContext) createQueueSchedulingReports() {
	c.queueSchedulingReportByQueue = make(map[string]*queueSchedulingReport)
	for _, roundReport := range c.queueSchedulingRoundReports {
		if report, ok := c.queueSchedulingReportByQueue[roundReport.queueName]; ok {
			report.numTimesSelected++
			report.numJobsLeased += roundReport.numJobsLeased
			report.leasedResources.Add(roundReport.leasedResources)
			maps.Copy(report.unschedulableReasonByJobId, roundReport.unschedulableReasonByJobId)
		} else {
			report = &queueSchedulingReport{
				created:                          roundReport.created,
				queueName:                        roundReport.queueName,
				initialQueueSelectionProbability: roundReport.queueSelectionProbability,
				initialResourceShare:             roundReport.resourceShare,
				numTimesSelected:                 1,
				numJobsLeased:                    roundReport.numJobsLeased,
				leasedResources:                  roundReport.leasedResources.DeepCopy(),
				unschedulableReasonByJobId:       maps.Clone(roundReport.unschedulableReasonByJobId),
			}
			c.queueSchedulingReportByQueue[roundReport.queueName] = report
		}
	}
}

// LeaseJobs is the point of entry for requesting jobs to be leased to an executor.
// I.e., this function is called from the lease request gRPC endpoint.
// This function creates a leaseContext (a struct composed of all info necessary for scheduling)
// and calls leaseContext.scheduleJobs, which returns a list of leased jobs.
func LeaseJobs(ctx context.Context,
	config *configuration.SchedulingConfig, // Scheduler settings.
	jobQueue JobQueue, // Job repository.
	onJobLease func([]*api.Job), // Function to be called for all leased jobs.
	request *api.LeaseRequest, // The gRPC lease request message.
	nodeResources []*nodeTypeAllocation, // Resources available per node type.
	activeClusterReports map[string]*api.ClusterUsageReport,
	activeClusterLeaseJobReports map[string]*api.ClusterLeasedReport,
	clusterPriorities map[string]map[string]float64,
	activeQueues []*api.Queue,
) ([]*api.Job, error) {
	lc := newLeaseContext(
		config,
		jobQueue,
		onJobLease,
		request,
		nodeResources,
		activeClusterReports,
		activeClusterLeaseJobReports,
		clusterPriorities,
		activeQueues,
	)

	schedulingLimit := newLeasePayloadLimit(config.MaximumJobsToSchedule, config.MaximumLeasePayloadSizeBytes, int(config.MaxPodSpecSizeBytes))

	jobs, err := lc.scheduleJobs(ctx, schedulingLimit)
	if err != nil {
		return nil, errors.Errorf("[LeaseJobs] error scheduling jobs: %s", err)
	}

	return jobs, nil
}

func newLeaseContext(
	config *configuration.SchedulingConfig,
	jobQueue JobQueue,
	onJobLease func([]*api.Job),
	request *api.LeaseRequest,
	nodeResources []*nodeTypeAllocation,
	activeClusterReports map[string]*api.ClusterUsageReport,
	activeClusterLeaseJobReports map[string]*api.ClusterLeasedReport,
	clusterPriorities map[string]map[string]float64,
	activeQueues []*api.Queue,
) *leaseContext {
	resourcesToSchedule := common.ComputeResources(request.Resources).AsFloat()
	currentClusterReport, ok := activeClusterReports[request.ClusterId]

	totalCapacity := &common.ComputeResources{}

	for _, clusterReport := range activeClusterReports {
		totalCapacity.Add(util.GetClusterAvailableCapacity(clusterReport))
	}

	log.Infof("=== newLeaseContext resourcesToSchedule: %v, totalCapacity: %v", resourcesToSchedule, totalCapacity)

	resourceAllocatedByQueue := CombineLeasedReportResourceByQueue(activeClusterLeaseJobReports)
	maxResourceToSchedulePerQueue := totalCapacity.MulByResource(config.MaximalResourceFractionToSchedulePerQueue)
	maxResourcePerQueue := totalCapacity.MulByResource(config.MaximalResourceFractionPerQueue)
	queueSchedulingInfo := calculateQueueSchedulingLimits(
		activeQueues,
		maxResourceToSchedulePerQueue,
		maxResourcePerQueue,
		totalCapacity,
		resourceAllocatedByQueue, // TODO: This one
	)

	if ok {
		capacity := util.GetClusterCapacity(currentClusterReport)
		resourcesToSchedule = resourcesToSchedule.LimitWith(capacity.MulByResource(config.MaximalClusterFractionToSchedule))
	}

	activeQueuePriority := CalculateQueuesPriorityInfo(clusterPriorities, activeClusterReports, activeQueues)
	scarcity := config.GetResourceScarcity(request.Pool)
	if scarcity == nil {
		scarcity = ResourceScarcityFromReports(activeClusterReports)
	}
	activeQueueSchedulingInfo := SliceResourceWithLimits(scarcity, queueSchedulingInfo, activeQueuePriority, resourcesToSchedule)

	leasedByQueue := make(map[string]*api.QueueLeasedReport)
	for _, report := range request.ClusterLeasedReport.Queues {
		leasedByQueue[report.Name] = report
	}

	return &leaseContext{
		schedulingConfig: config,
		queue:            jobQueue,

		clusterId: request.ClusterId,

		resourceScarcity:    scarcity,
		queueSchedulingInfo: activeQueueSchedulingInfo,
		priorities:          activeQueuePriority,

		leasedByQueue:  leasedByQueue,
		nodeResources:  nodeResources,
		minimumJobSize: request.MinimumJobSize,

		queueCache: map[string][]*api.Job{},

		onJobsLeased: onJobLease,

		schedulingReportByQueueName: make(map[string]*queueSchedulingRoundReport),
	}
}

func calculateQueueSchedulingLimits(
	activeQueues []*api.Queue,
	schedulingLimitPerQueue common.ComputeResourcesFloat,
	resourceLimitPerQueue common.ComputeResourcesFloat,
	totalCapacity *common.ComputeResources,
	currentQueueResourceAllocation map[string]common.ComputeResources,
) map[*api.Queue]*QueueSchedulingInfo {
	schedulingInfo := make(map[*api.Queue]*QueueSchedulingInfo, len(activeQueues))
	log.Infof("=== calculateQueueSchedulingLimits totalCapacity: %v", totalCapacity)
	for _, queue := range activeQueues {
		remainingGlobalLimit := resourceLimitPerQueue.DeepCopy()
		log.Infof("=== calculateQueueSchedulingLimits-0 remainingGlobalLimit: %v", remainingGlobalLimit)
		if len(queue.ResourceLimits) > 0 {
			customQueueLimit := totalCapacity.MulByResource(queue.ResourceLimits)
			remainingGlobalLimit = remainingGlobalLimit.MergeWith(customQueueLimit)
		}
		log.Infof("=== calculateQueueSchedulingLimits-1 remainingGlobalLimit: %v", remainingGlobalLimit)

		// TODO: This subtracts total queue resource usage.
		if usage, ok := currentQueueResourceAllocation[queue.Name]; ok {
			remainingGlobalLimit.Sub(usage.AsFloat())
			remainingGlobalLimit.LimitToZero()
		}
		log.Infof("=== calculateQueueSchedulingLimits-2 remainingGlobalLimit: %v", remainingGlobalLimit)

		schedulingRoundLimit := schedulingLimitPerQueue.DeepCopy()

		schedulingRoundLimit = schedulingRoundLimit.LimitWith(remainingGlobalLimit)
		log.Infof(
			"=== calculateQueueSchedulingLimits-3 remainingGlobalLimit: %v, schedulingRoundLimit: %v",
			remainingGlobalLimit, schedulingRoundLimit,
		)
		schedulingInfo[queue] = NewQueueSchedulingInfo(schedulingRoundLimit, common.ComputeResourcesFloat{}, common.ComputeResourcesFloat{})
	}
	return schedulingInfo
}

// TODO Remove logging code here. Instead, log at the gRPC handlers/interceptors with more info.
func (c *leaseContext) scheduleJobs(ctx context.Context, limit LeasePayloadLimit) ([]*api.Job, error) {
	var jobs []*api.Job

	if !c.schedulingConfig.UseProbabilisticSchedulingForAllResources {
		assignedJobs, err := c.assignJobs(ctx, limit)
		if err != nil {
			err = errors.Errorf("[leaseContext.scheduleJobs] error leasing jobs to cluster %s: %s", c.clusterId, err)
			log.Error(err)
			return nil, err
		}
		jobs = assignedJobs
		limit.RemoveFromRemainingLimit(jobs...)
	}

	additionalJobs, err := c.distributeRemainder(ctx, limit)
	if err != nil {
		err = errors.Errorf("[leaseContext.scheduleJobs] error leasing additional jobs to cluster %s: %s", c.clusterId, err)
		log.Error(err)
		return nil, err
	}
	jobs = append(jobs, additionalJobs...)

	if c.schedulingConfig.UseProbabilisticSchedulingForAllResources {
		log.WithField("clusterId", c.clusterId).Infof("Leasing %d jobs. (using probabilistic scheduling)", len(jobs))
	} else {
		log.WithField("clusterId", c.clusterId).Infof("Leasing %d jobs. (by remainder distribution: %d)", len(jobs), len(additionalJobs))
	}

	c.createQueueSchedulingReports()
	for _, report := range c.queueSchedulingReportByQueue {
		log.WithFields(report.asFields()).Info("scheduler finished")
	}

	return jobs, nil
}

func (c *leaseContext) assignJobs(ctx context.Context, limit LeasePayloadLimit) ([]*api.Job, error) {
	jobs := make([]*api.Job, 0)
	// TODO: parallelize
	for queue, info := range c.queueSchedulingInfo {
		// TODO: partition limit by priority instead
		partitionLimit := newLeasePayloadLimit(
			limit.remainingJobCount/len(c.queueSchedulingInfo),
			limit.remainingPayloadSizeLimitBytes/len(c.queueSchedulingInfo),
			limit.maxExpectedJobSizeBytes)
		leased, remainder, e := c.leaseJobs(ctx, queue, info.adjustedShare, partitionLimit)
		if e != nil {
			log.Error(e)
			continue
		}
		scheduled := info.adjustedShare.DeepCopy()
		scheduled.Sub(remainder)
		c.queueSchedulingInfo[queue].UpdateLimits(scheduled)
		jobs = append(jobs, leased...)

		if util.CloseToDeadline(ctx, leaseDeadlineTolerance) {
			break
		}
	}
	return jobs, nil
}

func (c *leaseContext) distributeRemainder(ctx context.Context, limit LeasePayloadLimit) ([]*api.Job, error) {
	var jobs []*api.Job
	if limit.AtLimit() {
		return jobs, nil
	}

	// Remaining resources that can be assigned to jobs.
	// Computed from per-node resource usage as reported in the StreamingLeaseJobs call.
	// Only accounts for resources assigned to non-preemptible jobs.
	remainingResourcesToSchedule := make(common.ComputeResourcesFloat)
	for _, node := range c.nodeResources {
		remainingResourcesToSchedule.Add(node.availableResources)
	}

	// // Remaining resources that can be assigned to jobs.
	// // c.queueSchedulingInfo is populated from the usage endpoints.
	// remainder := SumRemainingResource(c.queueSchedulingInfo)
	// for queue, info := range c.queueSchedulingInfo {
	// 	log.Infof("=== distributeRemainder queue: %v, info: %v", queue, info)
	// }

	// Fraction of remaining resources to assign to each queue.
	// Used to weight the probability of selecting a given queue.
	shares := QueueSlicesToShares(c.resourceScarcity, c.queueSchedulingInfo)
	log.Infof(
		"=== distributeRemainder remainder: %v, shares: %v, c.leasedByQueue: %v",
		remainingResourcesToSchedule, shares, c.leasedByQueue,
	)

	// When computing per-queue resource usage,
	// we only consider resources used by the highest-priority jobs.
	var maxPriority int32
	for _, priority := range c.schedulingConfig.Preemption.PriorityClasses {
		if priority > maxPriority {
			maxPriority = priority
		}
	}

	queueCount := len(c.queueSchedulingInfo)
	emptySteps := 0

	minimumJobSize := common.ComputeResources(c.minimumJobSize).AsFloat()
	minimumResource := c.schedulingConfig.MinimumResourceToSchedule.DeepCopy()
	minimumResource.Max(minimumJobSize)

	hasEnoughResources := !remainingResourcesToSchedule.IsLessThan(minimumResource)

	log.Infof(
		"=== distributeRemainder hasEnoughResources: %b, len(shares): %d, emptySteps: %d, queueCount: %d",
		hasEnoughResources, len(shares), emptySteps, queueCount,
	)

	for hasEnoughResources && len(shares) > 0 && emptySteps < queueCount {

		// Select a queue to schedule jobs from.
		// Queues with fewer resources allocated to them are selected with higher propability.
		queue, probability := pickQueueRandomly(shares)
		emptySteps++

		// The amount of resources to assign to jobs from this queue is the min between
		// the remaining resources and per-queue limits.
		amountToSchedule := remainingResourcesToSchedule.DeepCopy()
		amountToSchedule = amountToSchedule.LimitWith(c.queueSchedulingInfo[queue].remainingSchedulingLimit)

		// Add any resources consumed by preemptible jobs in this queue.
		// Since users should be allowed to schedule new jobs that preempt their preemptible jobs.
		//
		// TODO: This allows users to schedule preemptible jobs exceeding their fair share.
		preemptibleResources := make(common.ComputeResourcesFloat)
		if leased, ok := c.leasedByQueue[queue.Name]; ok {
			for priority, resources := range leased.ResourcesLeasedByPriority {
				if priority < maxPriority {
					log.Infof("=== distributeRemainder adding: %v", common.ComputeResources(resources.Resources).AsFloat())
					preemptibleResources.Add(common.ComputeResources(resources.Resources).AsFloat())
				}
			}
		}
		amountToSchedule.Add(preemptibleResources)

		// c.queueSchedulingInfo[queue]
		log.Infof("=== distributeRemainder amountToSchedule: %v", amountToSchedule)

		// Ensure the lease message sent to the executor does not exceed gRPC size limits.
		// TODO: We don't need this anymore. Since leases are streamed across now.
		leaseLimit := newLeasePayloadLimit(1, limit.remainingPayloadSizeLimitBytes, limit.maxExpectedJobSizeBytes)

		// Create the leases to be sent to the executor.
		leased, remaining, e := c.leaseJobs(ctx, queue, amountToSchedule, leaseLimit)
		if e != nil {
			log.Error(e)
			continue
		}
		var leasedResources common.ComputeResourcesFloat
		if len(leased) > 0 {
			emptySteps = 0
			jobs = append(jobs, leased...)

			leasedResources = amountToSchedule.DeepCopy()
			leasedResources.Sub(remaining)

			c.queueSchedulingInfo[queue].UpdateLimits(leasedResources)
			remainingResourcesToSchedule.Sub(leasedResources)
			shares[queue] = math.Max(0, ResourcesFloatAsUsage(c.resourceScarcity, c.queueSchedulingInfo[queue].schedulingShare))
		} else {
			// If there are no suitable jobs to lease for this queue,
			// do not consider it in the next loop iteration.
			delete(c.queueSchedulingInfo, queue)
			delete(c.priorities, queue)
			c.queueSchedulingInfo = SliceResourceWithLimits(c.resourceScarcity, c.queueSchedulingInfo, c.priorities, remainingResourcesToSchedule)
			shares = QueueSlicesToShares(c.resourceScarcity, c.queueSchedulingInfo)
		}

		// Create a scheduling report for this queue.
		report := &queueSchedulingRoundReport{
			created:                   time.Now(),
			queueName:                 queue.Name,
			queueSelectionProbability: probability,
			resourceShare:             amountToSchedule,
			preemptibleResources:      preemptibleResources,
			numJobsLeased:             len(leased),
			leasedResources:           leasedResources,
		}
		c.queueSchedulingRoundReports = append(c.queueSchedulingRoundReports, report)

		limit.RemoveFromRemainingLimit(leased...)
		if limit.AtLimit() || util.CloseToDeadline(ctx, leaseDeadlineTolerance) {
			break
		}
	}

	return jobs, nil
}

// leaseJobs calls into the JobRepository underlying the queue contained in the leaseContext to lease jobs.
// Returns a slice of jobs that were leased.
func (c *leaseContext) leaseJobs(
	ctx context.Context,
	queue *api.Queue,
	slice common.ComputeResourcesFloat,
	limit LeasePayloadLimit,
) ([]*api.Job, common.ComputeResourcesFloat, error) {

	log.Info("=== leaseContext.leaseJobs slice: %v", slice)

	// For each job we could not schedule, record the reason for it being unschedulable.
	unschedulableReasonByJobId := make(map[string]string)

	jobs := make([]*api.Job, 0)
	remainder := slice
	for slice.IsValid() {
		if limit.AtLimit() {
			break
		}

		// To avoid querying the database for jobs at each round,
		// only get a fresh batch of jobs from the scheduler if we've scheduled
		// at least half of the jobs we got in the last query.
		topJobs, ok := c.queueCache[queue.Name]
		if !ok || len(topJobs) < int(c.schedulingConfig.QueueLeaseBatchSize/2) {
			newTop, e := c.queue.PeekClusterQueue(c.clusterId, queue.Name, int64(c.schedulingConfig.QueueLeaseBatchSize))
			if e != nil {
				return nil, slice, e
			}
			c.queueCache[queue.Name] = newTop
			topJobs = c.queueCache[queue.Name]
		}

		candidates := make([]*api.Job, 0)
		candidatesLimit := newLeasePayloadLimit(limit.remainingJobCount, limit.remainingPayloadSizeLimitBytes, limit.maxExpectedJobSizeBytes)
		candidateNodes := map[*api.Job]nodeTypeUsedResources{}
		consumedNodeResources := nodeTypeUsedResources{}

		for _, job := range topJobs {

			// Subtract the resource requirements of this job from
			// the remaining resources to schedule for this queue.
			// If that becomes negative, we abort scheduling.
			requirement := common.TotalJobResourceRequest(job).AsFloat()
			remainder = slice.DeepCopy()
			remainder.Sub(requirement) // TODO: This should be computed from summing nodes.

			isJobLargeEnough := isLargeEnough(job, c.minimumJobSize)
			isRemainderValid := remainder.IsValid()
			isCandidateWithinLimit := candidatesLimit.IsWithinLimit(job)

			// Now we need to find a node type on which we can schedule the job.
			// And then subtract resources from that node type.

			if isJobSchedulable(c, job, remainder, candidatesLimit) {
				if hasPriorityClass(job.PodSpec) {
					validateOrDefaultPriorityClass(job.PodSpec, c.schedulingConfig.Preemption)
				}
				newlyConsumed, ok, _ := matchAnyNodeTypeAllocation(
					job,
					c.nodeResources,
					consumedNodeResources,
					c.schedulingConfig.Preemption.PriorityClasses,
				)
				if ok {
					slice = remainder
					candidates = append(candidates, job)
					candidatesLimit.RemoveFromRemainingLimit(job)
					candidateNodes[job] = newlyConsumed
					consumedNodeResources.Add(newlyConsumed)
				}
			}
			if candidatesLimit.AtLimit() {
				break
			}
		}
		c.queueCache[queue.Name] = removeJobs(c.queueCache[queue.Name], candidates)

		leased, e := c.queue.TryLeaseJobs(c.clusterId, queue.Name, candidates)
		if e != nil {
			return nil, slice, e
		}

		jobs = append(jobs, leased...)
		limit.RemoveFromRemainingLimit(leased...)

		// TODO: This should increase resources allocated by priority.
		c.decreaseNodeResources(leased, candidateNodes)

		// stop scheduling round if we leased less then batch (either the slice is too small or queue is empty)
		// TODO: should we look at next batch?
		if len(candidates) < int(c.schedulingConfig.QueueLeaseBatchSize) {
			break
		}
		if util.CloseToDeadline(ctx, leaseDeadlineTolerance) {
			break
		}
	}

	go c.onJobsLeased(jobs)

	return jobs, slice, nil
}

func isJobSchedulable(c *leaseContext, job *api.Job, remainder common.ComputeResourcesFloat, candidatesLimit LeasePayloadLimit) bool {
	isJobLargeEnough := isLargeEnough(job, c.minimumJobSize)
	isRemainderValid := remainder.IsValid()
	isCandidateWithinLimit := candidatesLimit.IsWithinLimit(job)
	isRegularlySchedulable := isJobLargeEnough && isRemainderValid && isCandidateWithinLimit

	log.Infof(
		"=== isJobSchedulable [%s] isJobLargeEnough: %v, isRemainderValid: %v, isCandidateWithinLimit: %v, remainder: %v",
		job.Id, isJobLargeEnough, isRemainderValid, isCandidateWithinLimit, remainder,
	)

	// isPreemptiveJob := isJobLargeEnough && hasPriorityClass(job.PodSpec)
	return isRegularlySchedulable // || isPreemptiveJob
}

// validateOrDefaultPriorityClass checks is the pod spec's priority class configured as supported in Server config
// if not, default to DefaultPriorityClass if it is specified
// otherwise default to no Priority Class
func validateOrDefaultPriorityClass(podSpec *v1.PodSpec, preemptionConfig configuration.PreemptionConfig) {
	if preemptionConfig.PriorityClasses == nil {
		podSpec.PriorityClassName = preemptionConfig.DefaultPriorityClass
	}
	_, ok := preemptionConfig.PriorityClasses[podSpec.PriorityClassName]
	if !ok {
		podSpec.PriorityClassName = preemptionConfig.DefaultPriorityClass
	}
}

func (c *leaseContext) decreaseNodeResources(leased []*api.Job, nodeTypeUsage map[*api.Job]nodeTypeUsedResources) {
	for _, j := range leased {
		for nodeType, resources := range nodeTypeUsage[j] {
			nodeType.availableResources.Sub(resources)
		}
	}
}

// removeJobs returns the subset of jobs not in jobsToRemove.
func removeJobs(jobs []*api.Job, jobsToRemove []*api.Job) []*api.Job {
	jobsToRemoveIds := make(map[string]bool, len(jobsToRemove))
	for _, job := range jobsToRemove {
		jobsToRemoveIds[job.Id] = true
	}

	result := make([]*api.Job, 0, len(jobs))
	for _, job := range jobs {
		if _, shouldRemove := jobsToRemoveIds[job.Id]; !shouldRemove {
			result = append(result, job)
		}
	}
	return result
}

// pickQueueRandomly returns a queue randomly selected from the provided map.
// The probability of returning a particular queue AQueue is shares[AQueue] / sharesSum,
// where sharesSum is the sum of all values in the provided map.
func pickQueueRandomly(shares map[*api.Queue]float64) (*api.Queue, float64) {
	sum := 0.0
	for _, share := range shares {
		sum += share
	}

	pick := sum * rand.Float64()
	current := 0.0

	var lastQueue *api.Queue
	lastShare := 0.0
	for queue, share := range shares {
		current += share
		if current >= pick {
			return queue, share / sum
		}
		lastQueue = queue
		lastShare = share
	}
	log.Error("Could not randomly pick a queue, this should not happen!")
	return lastQueue, lastShare / sum
}
