package fake

import (
	"testing"

	v1 "k8s.io/api/core/v1"

	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/executor/job"
	"github.com/G-Research/armada/pkg/api"
)

type MockLeaseService struct {
	ReturnLeaseCalls      int
	RequestJobLeasesCalls int
	ReportDoneCalls       int

	ReturnLeaseArg *v1.Pod
	ReportDoneArg  []string
}

func NewMockLeaseService() *MockLeaseService {
	return &MockLeaseService{0, 0, 0, nil, nil}
}

func (ls *MockLeaseService) RenewJobLeases(jobs []*job.RunningJob) ([]*job.RunningJob, error) {
	return []*job.RunningJob{}, nil
}

func (ls *MockLeaseService) ReturnLease(pod *v1.Pod) error {
	ls.ReturnLeaseArg = pod
	ls.ReturnLeaseCalls++
	return nil
}

func (ls *MockLeaseService) RequestJobLeases(availableResource *common.ComputeResources, nodes []api.NodeInfo, leasedResourceByQueue map[string]common.ComputeResources) ([]*api.Job, error) {
	ls.RequestJobLeasesCalls++
	return make([]*api.Job, 0), nil
}

func (ls *MockLeaseService) ReportDone(jobIds []string) error {
	ls.ReportDoneArg = jobIds
	ls.ReportDoneCalls++
	return nil
}

func (ls *MockLeaseService) AssertReportDoneCalledOnceWith(t *testing.T, expected []string) {
	assert.Equal(t, 1, ls.ReportDoneCalls)
	assert.Equal(t, expected, ls.ReportDoneArg)
}
