package fake

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"

	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/executor/job"
	"github.com/armadaproject/armada/pkg/api"
)

type MockLeaseService struct {
	NonrenewableJobIds    []string
	ReturnLeaseCalls      int
	RequestJobLeasesCalls int
	ReportDoneCalls       int

	ReturnLeaseArg *v1.Pod
	ReportDoneArg  []string
}

func NewMockLeaseService() *MockLeaseService {
	return &MockLeaseService{[]string{}, 0, 0, 0, nil, nil}
}

func (ls *MockLeaseService) RenewJobLeases(jobs []*job.RunningJob) ([]*job.RunningJob, error) {
	failedRenewJobs := []*job.RunningJob{}
	for _, j := range jobs {
		if util.ContainsString(ls.NonrenewableJobIds, j.JobId) {
			failedRenewJobs = append(failedRenewJobs, j)
		}
	}
	return failedRenewJobs, nil
}

func (ls *MockLeaseService) ReturnLease(pod *v1.Pod, reason string, jobRunAttempted bool) error {
	ls.ReturnLeaseArg = pod
	ls.ReturnLeaseCalls++
	return nil
}

func (ls *MockLeaseService) RequestJobLeases(
	availableResource *armadaresource.ComputeResources,
	nodes []api.NodeInfo,
	leasedResourceByQueue map[string]armadaresource.ComputeResources,
	leasedResourceByQueueAndPriority map[string]map[int32]armadaresource.ComputeResources,
) ([]*api.Job, error) {
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
