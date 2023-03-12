package fake

import (
	"time"

	"github.com/armadaproject/armada/internal/executor/job"
)

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

func (s *StubRunStateStore) ReportRunLeased(runMeta *job.RunMeta, j *job.SubmitJob) {
	s.JobRunState[runMeta.RunId] = &job.RunState{Meta: runMeta, Job: j, Phase: job.Leased, LastPhaseTransitionTime: time.Now()}
}

func (s *StubRunStateStore) ReportRunInvalid(runMeta *job.RunMeta) {
	s.JobRunState[runMeta.RunId] = &job.RunState{Meta: runMeta, Phase: job.Invalid, LastPhaseTransitionTime: time.Now()}
}

func (s *StubRunStateStore) ReportSuccessfulSubmission(runMeta *job.RunMeta) {
	run, ok := s.JobRunState[runMeta.RunId]
	if !ok {
		return
	}
	run.Phase = job.SuccessfulSubmission
}

func (s *StubRunStateStore) ReportFailedSubmission(runMeta *job.RunMeta) {
	run, ok := s.JobRunState[runMeta.RunId]
	if !ok {
		return
	}
	run.Phase = job.FailedSubmission
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

func (s *StubRunStateStore) GetAllWithFilter(fn func(state *job.RunState) bool) []*job.RunState {
	result := make([]*job.RunState, 0, len(s.JobRunState))
	for _, jobRun := range s.JobRunState {
		if fn(jobRun) {
			result = append(result, jobRun)
		}
	}
	return result
}
