package job

import "sync"

// TestJobRunStateStore Just wraps JobRunStateStore but allows tests to set the initial state
type TestJobRunStateStore struct {
	jobRunStateStore *JobRunStateStore
}

func NewTestJobRunStateStore(initialJobRuns []*RunState) *TestJobRunStateStore {
	stateStore := &JobRunStateStore{jobRunState: map[string]*RunState{}, lock: sync.Mutex{}}
	for _, jobRun := range initialJobRuns {
		stateStore.jobRunState[jobRun.Meta.RunId] = jobRun
	}
	return &TestJobRunStateStore{
		jobRunStateStore: stateStore,
	}
}

func (s *TestJobRunStateStore) SetState(runState map[string]*RunState) {
	s.jobRunStateStore.jobRunState = runState
}

func (s *TestJobRunStateStore) ReportRunLeased(runMeta *RunMeta, j *SubmitJob) {
	s.jobRunStateStore.ReportRunLeased(runMeta, j)
}

func (s *TestJobRunStateStore) ReportRunInvalid(runMeta *RunMeta) {
	s.jobRunStateStore.ReportRunInvalid(runMeta)
}

func (s *TestJobRunStateStore) ReportSuccessfulSubmission(runMeta *RunMeta) {
	s.jobRunStateStore.ReportSuccessfulSubmission(runMeta)
}

func (s *TestJobRunStateStore) ReportFailedSubmission(runMeta *RunMeta) {
	s.jobRunStateStore.ReportFailedSubmission(runMeta)
}

func (s *TestJobRunStateStore) RequestRunCancellation(runId string) {
	s.jobRunStateStore.RequestRunCancellation(runId)
}

func (s *TestJobRunStateStore) RequestRunPreemption(runId string) {
	s.jobRunStateStore.RequestRunPreemption(runId)
}

func (s *TestJobRunStateStore) Delete(runId string) {
	s.jobRunStateStore.Delete(runId)
}

func (s *TestJobRunStateStore) Get(runId string) *RunState {
	return s.jobRunStateStore.Get(runId)
}

func (s *TestJobRunStateStore) GetAll() []*RunState {
	return s.jobRunStateStore.GetAll()
}

func (s *TestJobRunStateStore) GetByKubernetesId(kubernetesId string) *RunState {
	return s.jobRunStateStore.GetByKubernetesId(kubernetesId)
}

func (s *TestJobRunStateStore) GetAllWithFilter(fn func(state *RunState) bool) []*RunState {
	return s.jobRunStateStore.GetAllWithFilter(fn)
}
