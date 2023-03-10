package fake

import "github.com/armadaproject/armada/internal/executor/job"

type StubRunStateStore struct {
	JobRunState map[string]*job.RunState
}

func NewStubRunStateStore(initialJobRuns []*job.RunState) *StubRunStateStore {
	state := map[string]*job.RunState{}
	for _, jobRun := range initialJobRuns {
		state[jobRun.Meta.RunId] = jobRun
	}
	return &StubRunStateStore{
		JobRunState: state,
	}
}

func (s *StubRunStateStore) ReportRunLeased(runMeta *job.RunMeta, job *job.SubmitJob) {
	panic("implement me")
}

func (s *StubRunStateStore) ReportRunInvalid(runMeta *job.RunMeta) {
	panic("implement me")
}

func (s *StubRunStateStore) ReportFailedSubmission(runMeta *job.RunMeta) {
	panic("implement me")
}

func (s *StubRunStateStore) RequestRunCancellation(runId string) {
	panic("implement me")
}

func (s *StubRunStateStore) RequestRunPreemption(runId string) {
	panic("implement me")
}

func (s *StubRunStateStore) Delete(runId string) {
	delete(s.JobRunState, runId)
}

func (s *StubRunStateStore) Get(runId string) *job.RunState {
	return s.JobRunState[runId]
}

func (s *StubRunStateStore) GetAll() []*job.RunState {
	result := make([]*job.RunState, 0, len(s.JobRunState))
	for _, jobRun := range s.JobRunState {
		result = append(result, jobRun)
	}
	return result
}

func (s *StubRunStateStore) GetByKubernetesId(kubernetesId string) *job.RunState {
	for _, jobRun := range s.JobRunState {
		if jobRun.KubernetesId == kubernetesId {
			return jobRun
		}
	}
	return nil
}

func (s *StubRunStateStore) GetByPhase(phase job.RunPhase) []*job.RunState {
	return s.GetWithFilter(func(state *job.RunState) bool {
		return state.Phase == phase
	})
}

func (s *StubRunStateStore) GetWithFilter(fn func(state *job.RunState) bool) []*job.RunState {
	result := make([]*job.RunState, 0, len(s.JobRunState))
	for _, jobRun := range s.JobRunState {
		if fn(jobRun) {
			result = append(result, jobRun)
		}
	}
	return result
}
