package job

import (
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/util"
)

type JobRunStateStore struct {
	// RunId -> RunState
	jobRunState    map[string]*RunState
	lock           sync.Mutex
	clusterContext context.ClusterContext
}

func NewJobRunStateStore(clusterContext context.ClusterContext) *JobRunStateStore {
	stateStore := &JobRunStateStore{
		jobRunState:    map[string]*RunState{},
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

	runMeta, err := ExtractJobRunMeta(pod)
	if err != nil {
		log.Errorf("Failed to record pod %s as active because %s", pod.Name, err)
		return
	}

	currentState, present := stateStore.jobRunState[runMeta.RunId]
	if !present {
		currentState = &RunState{
			Meta: runMeta,
		}
		stateStore.jobRunState[runMeta.RunId] = currentState
	}

	currentState.Phase = Active
	currentState.KubernetesId = string(pod.UID)
	currentState.Job = nil // Now that the job is active, remove the object to save memory
	currentState.LastPhaseTransitionTime = time.Now()
}

func (stateStore *JobRunStateStore) ReportRunLeased(runMeta *RunMeta, job *SubmitJob) {
	stateStore.lock.Lock()
	defer stateStore.lock.Unlock()
	_, present := stateStore.jobRunState[runMeta.RunId]
	if !present {
		state := &RunState{
			Meta:                    runMeta,
			Job:                     job,
			Phase:                   Leased,
			CancelRequested:         false,
			LastPhaseTransitionTime: time.Now(),
		}
		stateStore.jobRunState[runMeta.RunId] = state
	} else {
		log.Warnf("run unexpectedly reported as leased (runId=%s, jobId=%s), state already exists", runMeta.RunId, runMeta.JobId)
	}
}
func (stateStore *JobRunStateStore) ReportRunInvalid(runMeta *RunMeta) {
	stateStore.lock.Lock()
	defer stateStore.lock.Unlock()
	_, present := stateStore.jobRunState[runMeta.RunId]
	if !present {
		state := &RunState{
			Meta:                    runMeta,
			Phase:                   Invalid,
			CancelRequested:         false,
			LastPhaseTransitionTime: time.Now(),
		}
		stateStore.jobRunState[runMeta.RunId] = state
	} else {
		log.Warnf("run unexpectedly reported as invalid (runId=%s, jobId=%s), state already exists", runMeta.RunId, runMeta.JobId)
	}
}

func (stateStore *JobRunStateStore) ReportFailedSubmission(runMeta *RunMeta) {
	stateStore.lock.Lock()
	defer stateStore.lock.Unlock()

	currentState, present := stateStore.jobRunState[runMeta.RunId]
	if !present {
		log.Warnf("run unexpected reported as failed submission (runId=%s, jobId=%s), no current state exists", runMeta.RunId, runMeta.JobId)
		currentState = &RunState{
			Meta: runMeta,
		}
		stateStore.jobRunState[runMeta.RunId] = currentState
	}
	currentState.Phase = FailedSubmission
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

func (stateStore *JobRunStateStore) Get(runId string) *RunState {
	stateStore.lock.Lock()
	defer stateStore.lock.Unlock()

	return stateStore.jobRunState[runId].DeepCopy()
}

func (stateStore *JobRunStateStore) GetAll() []*RunState {
	stateStore.lock.Lock()
	defer stateStore.lock.Unlock()

	result := make([]*RunState, 0, len(stateStore.jobRunState))
	for _, jobRun := range stateStore.jobRunState {
		result = append(result, jobRun.DeepCopy())
	}
	return result
}

func (stateStore *JobRunStateStore) GetByKubernetesId(kubernetesId string) *RunState {
	stateStore.lock.Lock()
	defer stateStore.lock.Unlock()

	for _, run := range stateStore.jobRunState {
		if run.KubernetesId == kubernetesId {
			return run.DeepCopy()
		}
	}
	return nil
}

func (stateStore *JobRunStateStore) GetByPhase(phase RunPhase) []*RunState {
	return stateStore.GetWithFilter(func(state *RunState) bool {
		return state.Phase == phase
	})
}

func (stateStore *JobRunStateStore) GetWithFilter(fn func(state *RunState) bool) []*RunState {
	stateStore.lock.Lock()
	defer stateStore.lock.Unlock()

	result := make([]*RunState, 0, len(stateStore.jobRunState))
	for _, jobRun := range stateStore.jobRunState {
		if fn(jobRun) {
			result = append(result, jobRun.DeepCopy())
		}
	}
	return result
}
