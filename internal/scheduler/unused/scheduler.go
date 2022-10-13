package scheduler

// Scheduler contains the current state of the Armada scheduler.
//
// Armada uses a two-stage scheduling approach, consisting of an Armada-level meta-scheduler and a set of per-cluster
// schedulers, one per Kubernetes cluster. The meta-scheduler is responsible for assigning jobs to Kubernetes clusters
// and the per-cluster schedulers are responsible for assigning those jobs to nodes.
//
// A set of executors- one per cluster- is responsible for polling the meta-scheduler and reconciling any differences
// between the state of the meta-scheduler and the per-cluster schedulers by preempting jobs or submitting jobs to the
// per-cluster schedulers. Here, we describe the meta-scheduler, which we refer to as "the scheduler".
//
// The meta-scheduler subsystem consists of three components:
//   - The scheduler, which is responsible for assigning jobs to executors.
//     For performance, relies only on in-memory storage during normal operation.
//     Submits scheduling decisions to the log in the form of lease messages, i.e., tuples (jobId, executorId).
//   - The query API, which is responsible for returning leases associated with an executor.
//     Relies on a postgres database for storage.
//   - The log processor, which is responsible for updating the in-memory storage of the scheduler and postgres
//     based on log messages.
//
// During startup, the scheduler initialises its in-memory storage from postgres.
//
// The scheduler greedily assigns jobs to clusters one a time according to the following principles:
// - Divide cluster resources (i.e., CPU, RAM, and accelerators) fairly between queues.
// - For each queue, assign jobs to executors in order of priority.
// - Choose the executor to which each job is assigned to reserve resources for large incoming jobs.
//
// The scheduler continually tracks the total amount of resources assigned to each executor, i.e., the total amount of
// resources claimed by all jobs assigned to each executor, and queue, i.e., the total amount of resources claimed by jobs
// originating from each queue. Executor resource usage determines which clusters are considered for scheduling. Queue
// resource usage determines which queue to schedule from next.
//
// We oversubscribe clusters and each cluster has a lower and upper oversubscription threshold associated with it.
// Only clusters that fall below this threshold are considered for scheduling, and we only assign a job to an
// executor if doing so would not mean exceeding its upper threshold.
//
// To divide resources fairly, the scheduler always considers the queue furthest below its target resource usage for
// which the highest priority job can be assigned to some executor.
//
// At a high level, the scheduler operates in one of two modes. If the cluster is undersubscribed (i.e., jobs can be run
// immediately), jobs are assigned to executors using a bin packing strategy to maximise density. If the cluster is
// oversubscribed (i.e., jobs need to be queued), jobs are assigned to executors to minimise resource contention. In both
// cases, this is to increase the chance that large incoming jobs can be scheduled in a timely fashion.
//
// The scheduling algorithm works as follows:
//  1. Select the queue furthest below its target resource usage.
//  2. Select the highest priority job from that queue.
//  3. Filter out all clusters the job could never be run on.
//     If no clusters remain, mark the job as unschedulable, remove it from the queue, and exit.
//  4. Filter out all clusters above its lower resource usage threshold.
//     If no clusters remain, sleep for a bit before restarting the scheduling process.
//     If the job can't be scheduled on any cluster without exceeding its upper threshold,
//     temporarily remove the queue from the scheduler and exit.
//  5. Select all clusters on which the job could be run immediately (i.e., all sufficiently undersubscribed clusters).
//     If this list is non-empty, assign the job to the most full such cluster (using a bin packing strategy) and exit.
//  6. Order the list of clusters from step 4 by resource contention and assign the job to the
//     cluster with the lowest resource contention. Note that, at this point, assigning the job to any cluster results in
//     it becoming oversubscribed if it was not already.
//
// In addition, the scheduler filters out jobs using the following rules:
//   - For each resource, no job may claim more than some fraction of the total amount of resources.
//   - No queue may be assigned more than some fraction of the total amount of cluster resources.
//   - Jobs marked as "fragile" may only be assigned to a cluster at most once and may not be preempted.
//     Non-fragile jobs may be re-assigned on failure and may be preempted.
//
// When the scheduler assigns a job to a cluster, it simultaneously updates its in-memory state and submits a "lease"
// message, i.e., a tuple (job_id, executor_id), to the log. The lease is only visible via the query API once the
// message has made it through the log and been written to postgres by the log processor.
//
// TODO: Figure out how the executor + cluster-scheduler (likely the standard k8s scheduler) subsystem will work.

// type Scheduler struct {
// 	// Complete list of queues that jobs are scheduled from.
// 	queues []*Queue
// 	// Priority queue of queues considered for scheduling.
// 	pq QueuePQ
// 	// Queues not currently considered for scheduling.
// 	// Queues are placed here temporarily if their highest priority job can't be scheduled.
// 	paused []*Queue
// 	// Connected executor clusters.
// 	clusters []*Cluster
// 	// Subset of clusters eligible for scheduling.
// 	// schedulableClusters []*Queue
// 	// Lock protecting all fields of this struct.
// 	lock *sync.Mutex
// }

// type Cluster struct {
// 	Id string
// 	// Used to determine which clusters each job can be assigned to.
// 	// Total amount of resources available across the cluster.
// 	TotalResources map[string]int64
// 	// For each resource, the maximum available on any single node.
// 	MaxResources map[string]int64
// 	// Total amount of resources claimed by the jobs leased to the cluster.
// 	ClaimedResources map[string]int64
// }

// // Representing a job lease, i.e., the assignment of a job to a cluster.
// type Lease struct {
// 	Id         string
// 	JobId      string
// 	ExecutorId string
// }

// // scheduleForever continually schedules jobs.
// func (s *Scheduler) scheduleForever() {
// 	for {
// 		err := s.scheduleOne()
// 		if err != nil {
// 			fmt.Printf("error: %s\n", err)
// 		}
// 		// TODO: Look at the error code to determine any changes that need to be made.
// 	}
// }

// // scheduleOne schedules one job, or returns an error.
// func (s *Scheduler) scheduleOne() error {
// 	queue, err := s.selectQueue()
// 	if err != nil {
// 		return err
// 	}

// 	job, err := queue.selectJob()
// 	if err != nil {
// 		return err
// 	}

// 	lease, err := s.suggestLease(queue, job)
// 	if err != nil {
// 		return err
// 	}
// 	fmt.Printf("suggested lease: %s\n", lease)
// 	return nil
// }

// // selectQueue returns the highest priority queue from the scheduler priority queue.
// // Does not remove the queue from the priority queue.
// func (s *Scheduler) selectQueue() (*Queue, error) {
// 	if s.pq.Len() == 0 {
// 		err := errors.Errorf("no queues available")
// 		return nil, errors.WithStack(err)
// 	}
// 	return s.pq[0], nil
// }

// // selectJob returns the highest priority job from the queue priority queue.
// // Does not remove the queue from the priority queue.
// func (q *Queue) selectJob() (*Job, error) {
// 	if q.Pq.Len() == 0 {
// 		err := errors.Errorf("no jobs available")
// 		return nil, errors.WithStack(err)
// 	}
// 	return q.Pq[0], nil
// }

// // suggestLease returns a suggested scheduling decision (i.e., a lease), or an error.
// // The caller is responsible for acting on that decision (or not) and updating the scheduler data structures accordingly.
// func (s *Scheduler) suggestLease(queue *Queue, job *Job) (*Lease, error) {
// 	s.lock.Lock()
// 	defer s.lock.Unlock()
// 	if len(s.clusters) == 0 {
// 		err := errors.Errorf("no clusters available")
// 		return nil, errors.WithStack(err)
// 	}

// 	// Select the clusters the job could possibly run on.
// 	eligible := FilterIneligibleClusters(job, s.clusters)
// 	if len(eligible) == 0 {
// 		// The job can never be run on any connected cluster.
// 		err := errors.Errorf("no eligible clusters for job")
// 		return nil, errors.WithStack(err)
// 	}

// 	// Select the subset of those the job could be assigned to now.
// 	available := FilterUnavailableClusters(job, eligible)
// 	if len(available) == 0 {
// 		// It may be possible to schedule the job later.
// 		err := errors.Errorf("no currently available clusters for job")
// 		return nil, errors.WithStack(err)
// 	}

// 	// Of the available clusters, determine which cluster to assign the job to.
// 	cluster, err := selectCluster(job, available)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return &Lease{
// 		Id:         uuid.New().String(),
// 		JobId:      job.Id,
// 		ExecutorId: cluster.Id,
// 	}, nil
// }

// // FilterIneligibleClusters filters out any clusters on which the job could never be scheduled,
// // e.g., because it lacks required accelerators.
// func FilterIneligibleClusters(job *Job, clusters []*Cluster) []*Cluster {
// 	rv := make([]*Cluster, 0)
// 	for _, cluster := range clusters {
// 		if cluster.CanRun(job) {
// 			rv = append(rv, cluster)
// 		}
// 	}
// 	return clusters
// }

// // FilterUnavailableClusters filters out any clusters on which the job can't be scheduled right now,
// // e.g., because it is too busy.
// func FilterUnavailableClusters(job *Job, clusters []*Cluster) []*Cluster {
// 	rv := make([]*Cluster, 0)
// 	for _, cluster := range clusters {
// 		if cluster.CanRunNow(job) {
// 			rv = append(rv, cluster)
// 		}
// 	}
// 	return clusters
// }

// // CanRun returns true if the cluster could run the provided job, i.e.,
// // if it has sufficient resources available.
// func (c *Cluster) CanRun(job *Job) bool {
// 	for resource, requiredAmount := range job.Claims {
// 		amount, ok := c.TotalResources[resource]
// 		if !ok || amount < requiredAmount {
// 			return false
// 		}
// 		amount, ok = c.MaxResources[resource]
// 		if !ok || amount < requiredAmount {
// 			return false
// 		}
// 	}
// 	return true
// }

// // CanRunNow returns true if the cluster has sufficient available resources to run the job now.
// // Depending on how the available resources are spread across the nodes of the cluster,
// // the job may still need to be queued.
// func (c *Cluster) CanRunNow(job *Job) bool {
// 	for resource, requiredAmount := range job.Claims {
// 		amount, ok := c.TotalResources[resource]
// 		if !ok {
// 			return false
// 		}
// 		claimed, ok := c.ClaimedResources[resource]
// 		if !ok {
// 			return false
// 		}
// 		if amount-claimed < requiredAmount {
// 			return false
// 		}
// 	}
// 	return true
// }

// // selectCluster returns the cluster best suited for scheduling the given job.
// func selectCluster(job *Job, candidates []*Cluster) (*Cluster, error) {
// 	if len(candidates) == 0 {
// 		err := errors.Errorf("no clusters available to select from")
// 		return nil, errors.WithStack(err)
// 	}
// 	cluster := candidates[0]
// 	score := cluster.Score(job)
// 	for _, candidate := range candidates[1:] {
// 		candidateScore := candidate.Score(job)
// 		if candidateScore > score {
// 			cluster = candidate
// 			score = candidateScore
// 		}
// 	}
// 	return cluster, nil
// }

// // Score returns an integer that indicates how suitable the cluster is for scheduling the given job.
// // A higher value indicates that the cluster is more suitable.
// func (cluster *Cluster) Score(job *Job) int {
// 	return 0
// }
