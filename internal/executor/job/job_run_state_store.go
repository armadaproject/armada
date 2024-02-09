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

type RunStateStore interface {
	ReportRunLeased(runMeta *RunMeta, job *SubmitJob)
	ReportRunInvalid(runMeta *RunMeta)
	ReportSuccessfulSubmission(runId string)
	ReportFailedSubmission(runId string)
	RequestRunCancellation(runId string)
	RequestRunPreemption(runId string)
	Delete(runId string)
	Get(runId string) *RunState
	GetAll() []*RunState
	GetAllWithFilter(fn func(state *RunState) bool) []*RunState
	GetByKubernetesId(kubernetesId string) *RunState
}

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

			if !util.IsPodFinishedAndReported(pod) {
				stateStore.reportRunActive(pod)
			}
		},
	})

	// On start up, make sure our state matches current k8s state
	err := stateStore.initialiseStateFromKubernetes()
	if err != nil {
		panic(err)
	}
	return stateStore
}

// NewJobRunStateStoreWithInitialState This constructor is only intended for tests - as it does not reconcile with kubernetes state
func NewJobRunStateStoreWithInitialState(initialJobRuns []*RunState) *JobRunStateStore {
	stateStore := &JobRunStateStore{
		jobRunState: map[string]*RunState{},
		lock:        sync.Mutex{},
	}
	for _, jobRun := range initialJobRuns {
		stateStore.jobRunState[jobRun.Meta.RunId] = jobRun
	}
	return stateStore
}

func (stateStore *JobRunStateStore) initialiseStateFromKubernetes() error {
	pods, err := stateStore.clusterContext.GetAllPods()
	if err != nil {
		return err
	}
	for _, pod := range pods {
		if !util.IsPodFinishedAndReported(pod) {
			stateStore.reportRunActive(pod)
		}
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
			LastPhaseTransitionTime: time.Now(),
		}
		stateStore.jobRunState[runMeta.RunId] = state
	} else {
		log.Warnf("run unexpectedly reported as invalid (runId=%s, jobId=%s), state already exists", runMeta.RunId, runMeta.JobId)
	}
}

func (stateStore *JobRunStateStore) ReportSuccessfulSubmission(runId string) {
	stateStore.lock.Lock()
	defer stateStore.lock.Unlock()

	currentState, present := stateStore.jobRunState[runId]
	if !present {
		log.Warnf("run %s unexpectedly reported as successful submission, no run with that id exists", runId)
		return
	}
	currentState.Phase = SuccessfulSubmission
	currentState.LastPhaseTransitionTime = time.Now()
}

func (stateStore *JobRunStateStore) ReportFailedSubmission(runId string) {
	stateStore.lock.Lock()
	defer stateStore.lock.Unlock()

	currentState, present := stateStore.jobRunState[runId]
	if !present {
		log.Warnf("run %s unexpectedly reported as failed submission, no run with that id exists", runId)
		return
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

	run, exists := stateStore.jobRunState[runId]
	if !exists {
		return nil
	}
	return run.DeepCopy()
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

func (stateStore *JobRunStateStore) GetAllWithFilter(fn func(state *RunState) bool) []*RunState {
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
