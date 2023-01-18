package job

import (
	"fmt"
	"sync"
	"time"

	"github.com/armadaproject/armada/pkg/api"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/podchecks"
	"github.com/armadaproject/armada/internal/executor/util"
)

type IssueType int

const (
	UnableToSchedule  IssueType = iota
	StuckStartingUp   IssueType = iota
	StuckTerminating  IssueType = iota
	ExternallyDeleted IssueType = iota
)

type RunningJob struct {
	JobId      string
	ActivePods []*v1.Pod
	Issue      *PodIssue
}

type PodIssue struct {
	OriginatingPod *v1.Pod
	Pods           []*v1.Pod
	Message        string
	Retryable      bool
	Reported       bool
	Type           IssueType
	Cause          api.Cause
}

type jobRecord struct {
	jobId string
	issue *PodIssue
}

type JobContext interface {
	GetJobs() ([]*RunningJob, error)
	MarkIssueReported(issue *PodIssue)
	MarkIssuesResolved(job *RunningJob)
	DeleteJobWithCondition(job *RunningJob, condition func(pod *v1.Pod) bool) error
	DeleteJobs(jobs []*RunningJob)
	AddAnnotation(jobs []*RunningJob, annotations map[string]string)
}

type ClusterJobContext struct {
	clusterContext            context.ClusterContext
	stuckTerminatingPodExpiry time.Duration
	pendingPodChecker         podchecks.PodChecker
	updateThreadCount         int

	activeJobs        map[string]*jobRecord
	activeJobIdsMutex sync.Mutex
}

func NewClusterJobContext(
	clusterContext context.ClusterContext,
	pendingPodChecker podchecks.PodChecker,
	stuckTerminatingPodExpiry time.Duration,
	updateThreadCount int,
) *ClusterJobContext {
	jobContext := &ClusterJobContext{
		clusterContext:            clusterContext,
		stuckTerminatingPodExpiry: stuckTerminatingPodExpiry,
		pendingPodChecker:         pendingPodChecker,
		updateThreadCount:         updateThreadCount,
		activeJobs:                map[string]*jobRecord{},
		activeJobIdsMutex:         sync.Mutex{},
	}

	clusterContext.AddPodEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: func(obj interface{}) {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				log.Errorf("Failed to process pod event due to it being an unexpected type. Failed to process %+v", obj)
				return
			}
			jobContext.handleDeletedPod(pod)
		},
	})

	return jobContext
}

func (c *ClusterJobContext) GetJobs() ([]*RunningJob, error) {
	pods, err := c.clusterContext.GetActiveBatchPods()
	if err != nil {
		return nil, err
	}

	runningJobs := groupRunningJobs(pods)
	return c.addIssues(runningJobs), nil
}

func (c *ClusterJobContext) MarkIssuesResolved(job *RunningJob) {
	c.activeJobIdsMutex.Lock()
	defer c.activeJobIdsMutex.Unlock()

	record, exists := c.activeJobs[job.JobId]
	if exists {
		record.issue = nil
	}
}

func (c *ClusterJobContext) MarkIssueReported(issue *PodIssue) {
	issue.Reported = true
}

func (c *ClusterJobContext) DeleteJobWithCondition(job *RunningJob, condition func(pod *v1.Pod) bool) error {
	c.activeJobIdsMutex.Lock()
	defer c.activeJobIdsMutex.Unlock()

	for _, pod := range job.ActivePods {
		err := c.clusterContext.DeletePodWithCondition(pod, condition, true)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *ClusterJobContext) DeleteJobs(jobs []*RunningJob) {
	c.activeJobIdsMutex.Lock()
	defer c.activeJobIdsMutex.Unlock()

	for _, job := range jobs {
		c.clusterContext.DeletePods(job.ActivePods)
	}
}

func (c *ClusterJobContext) AddAnnotation(jobs []*RunningJob, annotations map[string]string) {
	podsToAnnotate := []*v1.Pod{}
	for _, job := range jobs {
		for _, pod := range job.ActivePods {
			podsToAnnotate = append(podsToAnnotate, pod)
		}
	}

	util.ProcessPodsWithThreadPool(podsToAnnotate, c.updateThreadCount,
		func(pod *v1.Pod) {
			err := c.clusterContext.AddAnnotation(pod, annotations)
			if err != nil {
				log.Warnf("Failed to annotate pod %s (%s) as done: %v", pod.Name, pod.Namespace, err)
			}
		},
	)
}

func groupRunningJobs(pods []*v1.Pod) []*RunningJob {
	podsByJobId := map[string][]*v1.Pod{}
	for _, pod := range pods {
		jobId := util.ExtractJobId(pod)
		podsByJobId[jobId] = append(podsByJobId[jobId], pod)
	}
	result := []*RunningJob{}
	for jobId, pods := range podsByJobId {
		result = append(result, &RunningJob{
			JobId:      jobId,
			ActivePods: pods,
		})
	}
	return result
}

func (c *ClusterJobContext) registerIssue(jobId string, issue *PodIssue) {
	record, exists := c.activeJobs[jobId]
	if !exists {
		record = &jobRecord{
			jobId: jobId,
		}
		c.activeJobs[jobId] = record
	}
	if record.issue == nil {
		record.issue = issue
	} else {
		log.Warnf("Not registering an issue for job %s as it already has an issue set", jobId)
	}
}

func (c *ClusterJobContext) addIssues(jobs []*RunningJob) []*RunningJob {
	c.activeJobIdsMutex.Lock()
	defer c.activeJobIdsMutex.Unlock()

	// register new jobs
	runningJobIds := map[string]*RunningJob{}
	for _, job := range jobs {
		runningJobIds[job.JobId] = job
		record, exists := c.activeJobs[job.JobId]
		if !exists {
			record = &jobRecord{
				jobId: job.JobId,
			}
			c.activeJobs[job.JobId] = record
		}
		if record.issue == nil {
			c.detectStuckPods(job)
		}
	}

	for jobId, record := range c.activeJobs {
		runningJob, isRunning := runningJobIds[jobId]
		if isRunning {
			runningJob.Issue = record.issue
		} else {
			if record.issue != nil {
				jobs = append(jobs, &RunningJob{
					JobId:      jobId,
					ActivePods: nil,
					Issue:      record.issue,
				})
			} else {
				delete(c.activeJobs, jobId)
			}
		}
	}
	return jobs
}

func (c *ClusterJobContext) detectStuckPods(runningJob *RunningJob) {
	for _, pod := range runningJob.ActivePods {
		if pod.DeletionTimestamp != nil && pod.DeletionTimestamp.Add(c.stuckTerminatingPodExpiry).Before(time.Now()) {
			// pod is stuck in terminating phase, this sometimes happen on node failure
			// it is safer to produce failed event than retrying as the job might have run already
			issue := &PodIssue{
				OriginatingPod: pod.DeepCopy(),
				Pods:           runningJob.ActivePods,
				Message:        "pod stuck in terminating phase, this might be due to platform problems",
				Retryable:      false,
				Type:           StuckTerminating,
			}
			runningJob.Issue = issue
			c.registerIssue(runningJob.JobId, issue)
			break

		} else if pod.Status.Phase == v1.PodUnknown || pod.Status.Phase == v1.PodPending {

			podEvents, err := c.clusterContext.GetPodEvents(pod)
			if err != nil {
				log.Errorf("Unable to get pod events for pod %s: %v", pod.Name, err)
			}

			lastStateChange, err := util.LastStatusChange(pod)
			if err != nil {
				log.Errorf("Unable to get lastStateChange for pod %s: %v", pod.Name, err)
				continue
			}

			action, cause, podCheckMessage := c.pendingPodChecker.GetAction(pod, podEvents, time.Now().Sub(lastStateChange))

			if action != podchecks.ActionWait {
				retryable := action == podchecks.ActionRetry
				message := createStuckPodMessage(retryable, podCheckMessage)
				podIssueType := StuckStartingUp
				if cause == podchecks.NoNodeAssigned {
					podIssueType = UnableToSchedule
				}

				log.Warnf("Found issue with pod %s in namespace %s: %s", pod.Name, pod.Namespace, message)

				issue := &PodIssue{
					OriginatingPod: pod.DeepCopy(),
					Pods:           runningJob.ActivePods,
					Message:        message,
					Retryable:      retryable,
					Type:           podIssueType,
				}
				runningJob.Issue = issue
				c.registerIssue(runningJob.JobId, issue)
				break
			}
		}
	}
}

func createStuckPodMessage(retryable bool, originalMessage string) string {
	if retryable {
		return fmt.Sprintf("Unable to schedule pod, Armada will return lease and retry.\n%s", originalMessage)
	}
	return fmt.Sprintf("Unable to schedule pod with unrecoverable problem, Armada will not retry.\n%s", originalMessage)
}

func (c *ClusterJobContext) handleDeletedPod(pod *v1.Pod) {
	c.activeJobIdsMutex.Lock()
	defer c.activeJobIdsMutex.Unlock()
	jobId := util.ExtractJobId(pod)
	if jobId != "" {
		isUnexpectedDeletion := !util.IsMarkedForDeletion(pod) && !util.IsPodFinishedAndReported(pod)
		if isUnexpectedDeletion {
			c.registerIssue(jobId, &PodIssue{
				OriginatingPod: pod,
				Pods:           []*v1.Pod{pod},
				Message:        "Pod of the active job was deleted.",
				Retryable:      false,
				Reported:       false,
				Type:           ExternallyDeleted,
			})
		}
	}
}
