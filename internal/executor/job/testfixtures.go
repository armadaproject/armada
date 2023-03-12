package job

import "time"

type StubRunStateStore struct {
	JobRunState map[string]*RunState
}

func NewStubRunStateStore(initialJobRuns []*RunState) *StubRunStateStore {
	state := map[string]*RunState{}
	for _, jobRun := range initialJobRuns {
		state[jobRun.Meta.RunId] = jobRun
	}
	return &StubRunStateStore{
		JobRunState: state,
	}
}

func (s *StubRunStateStore) ReportRunLeased(runMeta *RunMeta, j *SubmitJob) {
	s.JobRunState[runMeta.RunId] = &RunState{Meta: runMeta, Job: j, Phase: Leased, LastPhaseTransitionTime: time.Now()}
}

func (s *StubRunStateStore) ReportRunInvalid(runMeta *RunMeta) {
	s.JobRunState[runMeta.RunId] = &RunState{Meta: runMeta, Phase: Invalid, LastPhaseTransitionTime: time.Now()}
}

func (s *StubRunStateStore) ReportSuccessfulSubmission(runMeta *RunMeta) {
	run, ok := s.JobRunState[runMeta.RunId]
	if !ok {
		return
	}
	run.Phase = SuccessfulSubmission
}

func (s *StubRunStateStore) ReportFailedSubmission(runMeta *RunMeta) {
	run, ok := s.JobRunState[runMeta.RunId]
	if !ok {
		return
	}
	run.Phase = FailedSubmission
}

func (s *StubRunStateStore) RequestRunCancellation(runId string) {
	run, ok := s.JobRunState[runId]
	if !ok {
		return
	}
	run.CancelRequested = true
}

func (s *StubRunStateStore) RequestRunPreemption(runId string) {
	run, ok := s.JobRunState[runId]
	if !ok {
		return
	}
	run.PreemptionRequested = true
}

func (s *StubRunStateStore) Delete(runId string) {
	delete(s.JobRunState, runId)
}

func (s *StubRunStateStore) Get(runId string) *RunState {
	return s.JobRunState[runId]
}

func (s *StubRunStateStore) GetAll() []*RunState {
	result := make([]*RunState, 0, len(s.JobRunState))
	for _, jobRun := range s.JobRunState {
		result = append(result, jobRun)
	}
	return result
}

func (s *StubRunStateStore) GetByKubernetesId(kubernetesId string) *RunState {
	for _, jobRun := range s.JobRunState {
		if jobRun.KubernetesId == kubernetesId {
			return jobRun
		}
	}
	return nil
}

func (s *StubRunStateStore) GetAllWithFilter(fn func(state *RunState) bool) []*RunState {
	result := make([]*RunState, 0, len(s.JobRunState))
	for _, jobRun := range s.JobRunState {
		if fn(jobRun) {
			result = append(result, jobRun)
		}
	}
	return result
}
