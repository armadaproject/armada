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

type JobRunStateManager struct {
	// RunId -> RunState
	jobRunState    map[string]*RunState
	lock           sync.Mutex
	clusterContext context.ClusterContext
}

func NewJobRunState(clusterContext context.ClusterContext) *JobRunStateManager {
	stateManager := &JobRunStateManager{
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
			stateManager.reportRunAlive(pod)
		},
	})

	// On start up, make sure our state matches current k8s state
	err := stateManager.reconcileStateWithKubernetes()
	if err != nil {
		panic(err)
	}
	return stateManager
}

// TODO handle Missing JobRuns
// Stuck in "Leased" with no pod for > 5 mins (should be safe - as it indicates we submitted it and it never arrived in k8s)
// Stuck in "Live" with no pod for > 5 mins
// - Should consider ones that have finished and had its pod deleted - possibly risky

func (jrs *JobRunStateManager) reconcileStateWithKubernetes() error {
	pods, err := jrs.clusterContext.GetAllPods()
	if err != nil {
		return err
	}
	for _, pod := range pods {
		jrs.reportRunAlive(pod)
	}

	return nil
}

func (jrs *JobRunStateManager) reportRunAlive(pod *v1.Pod) {
	jrs.lock.Lock()
	defer jrs.lock.Unlock()

	if !util.IsManagedPod(pod) {
		return
	}

	// TODO handle attributes missing
	runMeta := &RunMetaInfo{
		RunId:  util.ExtractJobRunId(pod),
		JobId:  util.ExtractJobId(pod),
		JobSet: util.ExtractJobSet(pod),
		Queue:  util.ExtractQueue(pod),
	}

	currentState, present := jrs.jobRunState[runMeta.RunId]
	if !present {
		currentState = &RunState{
			Meta: runMeta,
		}
		jrs.jobRunState[runMeta.RunId] = currentState
	}

	currentState.Phase = Active
	currentState.KubernetesId = string(pod.UID)
	currentState.LastTransitionTime = time.Now()
}

func (jrs *JobRunStateManager) ReportRunLeased(runMeta *RunMetaInfo) {
	jrs.lock.Lock()
	defer jrs.lock.Unlock()
	_, present := jrs.jobRunState[runMeta.RunId]
	if !present {
		state := &RunState{
			Meta:               runMeta,
			Phase:              Leased,
			LastTransitionTime: time.Now(),
		}
		jrs.jobRunState[runMeta.RunId] = state
	} else {
		log.Warnf("run unexpectedly reported as leased (runId=%s, jobId=%s), state already exists", runMeta.RunId, runMeta.JobId)
	}
}

func (jrs *JobRunStateManager) ReportFailedSubmission(runMeta *RunMetaInfo) {
	jrs.lock.Lock()
	defer jrs.lock.Unlock()

	currentState, present := jrs.jobRunState[runMeta.RunId]
	if !present {
		log.Warnf("run unexpected reported as failed submission (runId=%s, jobId=%s), no current state exists", runMeta.RunId, runMeta.JobId)
		currentState = &RunState{
			Meta: runMeta,
		}
		jrs.jobRunState[runMeta.RunId] = currentState
	}
	currentState.Phase = FailedSubmission
	currentState.LastTransitionTime = time.Now()
}

func (jrs *JobRunStateManager) Delete(runId string) {
	jrs.lock.Lock()
	defer jrs.lock.Unlock()

	delete(jrs.jobRunState, runId)
}

func (jrs *JobRunStateManager) Get(runId string) *RunState {
	jrs.lock.Lock()
	defer jrs.lock.Unlock()

	return jrs.jobRunState[runId].DeepCopy()
}

func (jrs *JobRunStateManager) GetAll() []*RunState {
	jrs.lock.Lock()
	defer jrs.lock.Unlock()

	result := make([]*RunState, 0, len(jrs.jobRunState))
	for _, jobRun := range jrs.jobRunState {
		result = append(result, jobRun.DeepCopy())
	}
	return result
}

func (jrs *JobRunStateManager) GetByKubernetesId(kubernetesId string) *RunState {
	jrs.lock.Lock()
	defer jrs.lock.Unlock()

	for _, run := range jrs.jobRunState {
		if run.KubernetesId == kubernetesId {
			return run.DeepCopy()
		}
	}
	return nil
}
