package state

import (
	"sync"
	"time"

	"github.com/armadaproject/armada/internal/executor/job"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/util"
)

type RunStateStore interface {
	ReportRunLeased(runMeta *job.RunMeta, job *job.SubmitJob)
	ReportRunInvalid(runMeta *job.RunMeta)
	ReportFailedSubmission(runMeta *job.RunMeta)
	RequestRunCancellation(runId string)
	RequestRunPreemption(runId string)
	Delete(runId string)
	Get(runId string) *job.RunState
	GetAll() []*job.RunState
	GetByKubernetesId(kubernetesId string) *job.RunState
	GetByPhase(phase job.RunPhase) []*job.RunState
	GetWithFilter(fn func(state *job.RunState) bool) []*job.RunState
}

type JobRunStateStore struct {
	// RunId -> RunState
	jobRunState    map[string]*job.RunState
	lock           sync.Mutex
	clusterContext context.ClusterContext
}

func NewJobRunStateStore(clusterContext context.ClusterContext) *JobRunStateStore {
	stateStore := &JobRunStateStore{
		jobRunState:    map[string]*job.RunState{},
		lock:           sync.Mutex{},
		clusterContext: clusterContext,
	}

	clusterContext.AddPodEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				log.Errorf("Failed to process pod event due to it being an unexpected type. Failed to process %+v", obj)
				return
			}
			stateStore.reportRunActive(pod)
		},
	})

	// On start up, make sure our state matches current k8s state
	err := stateStore.reconcileStateWithKubernetes()
	if err != nil {
		panic(err)
	}
	return stateStore
}

func (stateStore *JobRunStateStore) reconcileStateWithKubernetes() error {
	pods, err := stateStore.clusterContext.GetAllPods()
	if err != nil {
		return err
	}
	for _, pod := range pods {
		stateStore.reportRunActive(pod)
	}

	return nil
}

func (stateStore *JobRunStateStore) reportRunActive(pod *v1.Pod) {
	stateStore.lock.Lock()
	defer stateStore.lock.Unlock()

	if !util.IsManagedPod(pod) {
		return
	}

	runMeta, err := job.ExtractJobRunMeta(pod)
	if err != nil {
		log.Errorf("Failed to record pod %s as active because %s", pod.Name, err)
		return
	}

	currentState, present := stateStore.jobRunState[runMeta.RunId]
	if !present {
		currentState = &job.RunState{
			Meta: runMeta,
		}
		stateStore.jobRunState[runMeta.RunId] = currentState
	}

	currentState.Phase = job.Active
	currentState.KubernetesId = string(pod.UID)
	currentState.Job = nil // Now that the job is active, remove the object to save memory
	currentState.LastPhaseTransitionTime = time.Now()
}

func (stateStore *JobRunStateStore) ReportRunLeased(runMeta *job.RunMeta, job *job.SubmitJob) {
	stateStore.lock.Lock()
	defer stateStore.lock.Unlock()
	_, present := stateStore.jobRunState[runMeta.RunId]
	if !present {
		state := &job.RunState{
			Meta:                    runMeta,
			Job:                     job,
			Phase:                   job.Leased,
			CancelRequested:         false,
			LastPhaseTransitionTime: time.Now(),
		}
		stateStore.jobRunState[runMeta.RunId] = state
	} else {
		log.Warnf("run unexpectedly reported as leased (runId=%s, jobId=%s), state already exists", runMeta.RunId, runMeta.JobId)
	}
}
func (stateStore *JobRunStateStore) ReportRunInvalid(runMeta *job.RunMeta) {
	stateStore.lock.Lock()
	defer stateStore.lock.Unlock()
	_, present := stateStore.jobRunState[runMeta.RunId]
	if !present {
		state := &job.RunState{
			Meta:                    runMeta,
			Phase:                   job.Invalid,
			CancelRequested:         false,
			LastPhaseTransitionTime: time.Now(),
		}
		stateStore.jobRunState[runMeta.RunId] = state
	} else {
		log.Warnf("run unexpectedly reported as invalid (runId=%s, jobId=%s), state already exists", runMeta.RunId, runMeta.JobId)
	}
}

func (stateStore *JobRunStateStore) ReportFailedSubmission(runMeta *job.RunMeta) {
	stateStore.lock.Lock()
	defer stateStore.lock.Unlock()

	currentState, present := stateStore.jobRunState[runMeta.RunId]
	if !present {
		log.Warnf("run unexpected reported as failed submission (runId=%s, jobId=%s), no current state exists", runMeta.RunId, runMeta.JobId)
		currentState = &job.RunState{
			Meta: runMeta,
		}
		stateStore.jobRunState[runMeta.RunId] = currentState
	}
	currentState.Phase = job.FailedSubmission
	currentState.LastPhaseTransitionTime = time.Now()
}

func (stateStore *JobRunStateStore) RequestRunCancellation(runId string) {
	stateStore.lock.Lock()
	defer stateStore.lock.Unlock()

	if currentState, present := stateStore.jobRunState[runId]; present {
		currentState.CancelRequested = true
	}
}

func (stateStore *JobRunStateStore) RequestRunPreemption(runId string) {
	stateStore.lock.Lock()
	defer stateStore.lock.Unlock()

	if currentState, present := stateStore.jobRunState[runId]; present {
		currentState.PreemptionRequested = true
	}
}

func (stateStore *JobRunStateStore) Delete(runId string) {
	stateStore.lock.Lock()
	defer stateStore.lock.Unlock()

	delete(stateStore.jobRunState, runId)
}

func (stateStore *JobRunStateStore) Get(runId string) *job.RunState {
	stateStore.lock.Lock()
	defer stateStore.lock.Unlock()

	return stateStore.jobRunState[runId].DeepCopy()
}

func (stateStore *JobRunStateStore) GetAll() []*job.RunState {
	stateStore.lock.Lock()
	defer stateStore.lock.Unlock()

	result := make([]*job.RunState, 0, len(stateStore.jobRunState))
	for _, jobRun := range stateStore.jobRunState {
		result = append(result, jobRun.DeepCopy())
	}
	return result
}

func (stateStore *JobRunStateStore) GetByKubernetesId(kubernetesId string) *job.RunState {
	stateStore.lock.Lock()
	defer stateStore.lock.Unlock()

	for _, run := range stateStore.jobRunState {
		if run.KubernetesId == kubernetesId {
			return run.DeepCopy()
		}
	}
	return nil
}

func (stateStore *JobRunStateStore) GetByPhase(phase job.RunPhase) []*job.RunState {
	return stateStore.GetWithFilter(func(state *job.RunState) bool {
		return state.Phase == phase
	})
}

func (stateStore *JobRunStateStore) GetWithFilter(fn func(state *job.RunState) bool) []*job.RunState {
	stateStore.lock.Lock()
	defer stateStore.lock.Unlock()

	result := make([]*job.RunState, 0, len(stateStore.jobRunState))
	for _, jobRun := range stateStore.jobRunState {
		if fn(jobRun) {
			result = append(result, jobRun.DeepCopy())
		}
	}
	return result
}
