package mocks

import (
	"context"

	"github.com/armadaproject/armada/internal/executor/service"
)

type StubLeaseRequester struct {
	ReceivedLeaseRequests    []*service.LeaseRequest
	LeaseJobRunError         error
	LeaseJobRunLeaseResponse *service.LeaseResponse
}

func (s *StubLeaseRequester) LeaseJobRuns(ctx context.Context, request *service.LeaseRequest) (*service.LeaseResponse, error) {
	s.ReceivedLeaseRequests = append(s.ReceivedLeaseRequests, request)
	return s.LeaseJobRunLeaseResponse, s.LeaseJobRunError
}
