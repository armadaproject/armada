// This file specifies the new Armada scheduler.
// This is work in progress and everything here is subject to change.
package scheduler

// Scheduler encapsulates the state of the Armada scheduler.
//
// An Armada deployment consists of an Armada server and one or more Kubernetes worker clusters,
// each of which is managed by a process referred to as the "executor".
// Exactly one executor process runs in each Kubernetes cluster.
//
// Armada uses a two-stage scheduling approach, consisting of a top-level scheduler (part of the server), and a
// set of per-cluster schedulers- one per worker cluster. The per-cluster scheduler are part of the executor.
// The top-level scheduler is responsible for assigning jobs to executors and the per-cluster schedulers are
// responsible for assigning those jobs to nodes. We refer to the top-level scheduler simply as "the scheduler".
// Each job is submitted to a particular queue and a job set, and has a priority associated with it.
//
// The scheduler operates according to the following principles:
// - Divide cluster resources (i.e., CPU, RAM, and accelerators) fairly between queues.
// - For each queue, schedule jobs strictly in order of priority.
// - Let clusters drain partially before scheduling and pack jobs tightly to reserve space for large jobs.
//   Tuning the extent to which clusters drain provides a trade-off between cluster utilisation
//   and the size of jobs that can be scheduled reliably.
//
// Specifically, each queue has a weight associated with it that determines its target resource usage.
// Denote the per-queue weights by w_1, ..., w_N, where N is the total number of queues,
// and by w = sum(w_1, ..., w_N) their sum. The target resource usage of each queue is then w_1 / w, ..., w_N / w.
//
// Jobs can optionally specify the following parameters:
// - lifetime: Maximum lifespan of the job in seconds. Jobs that exceed their lifetime are preempted.
//   A value of 0 (the default) indicates infinite lifetime.
//   Specifying a lifetime can reduce scheduling delay, since jobs with a fixed lifetime can be scheduled
//   ahead of higher-priority jobs that won't be held up.
// - atMostOnce: If false (the default), Armada will attempt to run the job only once.
//   If true, Armada may retry running the job if it doesn't succeed.
// - preemptible: Indicates if Armada may preempt the job or not. The default is false.
//   Unlike for non-preemptible jobs, for which there are per-job and per-queue resource usage limits,
//   there are no limits for preemptible jobs, since these may be preempted to make room for incoming jobs.
// - concurrencySafe: If true, Armada may assign the job to multiple schedulers simultaneously.
//   Once one of the executors starts the job, the other replicas are cancelled.
//   This can reduce scheduling delay but may result in several executors running the job concurrently.
//
// Below, we outline the scheduler implementation.
//
// The scheduler service consists of the following components:
// - The scheduler, which is responsible for assigning jobs to excecutors.
//   Jobs are assigned to executors by submitting lease messages, consisting of tuples (jobId, executorId), to Pulsar.
//   For peformance, this component relies only on in-memory storage.
// - The log processor, which is responsible for updating the in-memory storage as well as a
//   postgres database based on log messages.
//
// Each worker cluster has associated with it a lower resource usage threshold.
// The scheduler activates when a cluster falls below its lower threshold (e.g., when 10% of its CPU is unused).
// Hence, if the threshold is 90% for a particular resource, only jobs consuming up to 10% of that resource can be scheduled reliably.
// Choosing a higher threshold increases cluster utilisation at the expense of decreasing the size of jobs that can be scheduled reliably.
// Once activated, the scheduler assigns jobs to the cluster according to the following procedure:
// 1. Add to the scheduler all queues with unscheduled jobs.
// 2. Select the queue furthest below its target resource usage.
// 3. Select the highest-priority job from this queue.
//    If scheduling this job would exceed any per-job or per-queue resource usage limits,
//    select the high-priority preemptible job instead, if such a job exists.
//    If assigning this job to the cluster would cause cluster resource usage to exceed 100%,
//    remove the selected queue from the scheduler and go to step 5.
// 4. Assign the selected job to the cluster.
//    If the queue contains no more jobs, remove it from the scheduler.
// 5. Repeat until the scheduler contains no queues.
//
// Recall that scheduling decisions are recorded in the form of lease messages,
// which are published to Pulsar and subsequently writte to a database table.
// Each row of this table is a tuple
// (jobId, executorId, Kubernetes object spec, Kubernetes object name, state).
// Hence, jobs containing several Kubernetes objects (e.g., a job with a pod and a service object),
// result in several rows being written to the database.
//
// Each executor is responsible for polling the database and reconciling any differences between
// the list of objects written to it with the objects that currently exist in Kubernetes,
// creating and deleting objects as necessary (using the name to map database rows to objects in Kubernetes).
// The executor relies on the Kubernetes scheduler to assign pods to nodes.
// Finally, the executor writes the current state of each object back to the database.
type Scheduler struct {
}
