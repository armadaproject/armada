package service

import "context"

type StubLeaseRequester struct {
	ReceivedLeaseRequests    []*LeaseRequest
	LeaseJobRunError         error
	LeaseJobRunLeaseResponse *LeaseResponse
}

func (s *StubLeaseRequester) LeaseJobRuns(ctx context.Context, request *LeaseRequest) (*LeaseResponse, error) {
	s.ReceivedLeaseRequests = append(s.ReceivedLeaseRequests, request)
	return s.LeaseJobRunLeaseResponse, s.LeaseJobRunError
}
