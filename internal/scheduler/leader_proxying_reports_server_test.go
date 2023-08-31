package scheduler

import (
	gocontext "context"
	"fmt"
	"github.com/armadaproject/armada/internal/common/context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

func TestLeaderProxyingSchedulingReportsServer_GetJobReports(t *testing.T) {
	tests := map[string]struct {
		err                          error
		isCurrentProcessLeader       bool
		expectedNumReportServerCalls int
		expectedNumReportClientCalls int
	}{
		// Should send all requests to local reports server when leader
		"current process leader": {
			err:                          nil,
			isCurrentProcessLeader:       true,
			expectedNumReportServerCalls: 1,
			expectedNumReportClientCalls: 0,
		},
		"current process leader return error": {
			err:                          fmt.Errorf("error"),
			isCurrentProcessLeader:       true,
			expectedNumReportServerCalls: 1,
			expectedNumReportClientCalls: 0,
		},
		// Should send all requests to remote server when not leader
		"remote process is leader": {
			err:                          nil,
			isCurrentProcessLeader:       false,
			expectedNumReportServerCalls: 0,
			expectedNumReportClientCalls: 1,
		},
		"remote process is leader return error": {
			err:                          fmt.Errorf("error"),
			isCurrentProcessLeader:       false,
			expectedNumReportServerCalls: 0,
			expectedNumReportClientCalls: 1,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			sut, clientProvider, jobReportsServer, jobReportsClient := setupLeaderProxyingSchedulerReportsServerTest(t)
			clientProvider.IsCurrentProcessLeader = tc.isCurrentProcessLeader

			request := &schedulerobjects.JobReportRequest{JobId: "job-1"}

			expectedResult := &schedulerobjects.JobReport{Report: "report"}

			if tc.err == nil {
				expectedResult = nil
			}

			jobReportsServer.GetJobReportResponse = expectedResult
			jobReportsServer.Err = tc.err
			jobReportsClient.GetJobReportResponse = expectedResult
			jobReportsClient.Err = tc.err

			result, err := sut.GetJobReport(ctx, request)

			assert.Equal(t, tc.err, err)
			assert.Equal(t, expectedResult, result)
			assert.Len(t, jobReportsServer.GetJobReportCalls, tc.expectedNumReportServerCalls)
			assert.Len(t, jobReportsClient.GetJobReportCalls, tc.expectedNumReportClientCalls)
		})
	}
}

func TestLeaderProxyingSchedulingReportsServer_GetSchedulingReport(t *testing.T) {
	tests := map[string]struct {
		err                          error
		isCurrentProcessLeader       bool
		expectedNumReportServerCalls int
		expectedNumReportClientCalls int
	}{
		// Should send all requests to local reports server when leader
		"current process leader": {
			err:                          nil,
			isCurrentProcessLeader:       true,
			expectedNumReportServerCalls: 1,
			expectedNumReportClientCalls: 0,
		},
		"current process leader return error": {
			err:                          fmt.Errorf("error"),
			isCurrentProcessLeader:       true,
			expectedNumReportServerCalls: 1,
			expectedNumReportClientCalls: 0,
		},
		// Should send all requests to remote server when not leader
		"remote process is leader": {
			err:                          nil,
			isCurrentProcessLeader:       false,
			expectedNumReportServerCalls: 0,
			expectedNumReportClientCalls: 1,
		},
		"remote process is leader return error": {
			err:                          fmt.Errorf("error"),
			isCurrentProcessLeader:       false,
			expectedNumReportServerCalls: 0,
			expectedNumReportClientCalls: 1,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			sut, clientProvider, jobReportsServer, jobReportsClient := setupLeaderProxyingSchedulerReportsServerTest(t)
			clientProvider.IsCurrentProcessLeader = tc.isCurrentProcessLeader

			request := &schedulerobjects.SchedulingReportRequest{Verbosity: int32(1)}

			expectedResult := &schedulerobjects.SchedulingReport{Report: "report"}

			if tc.err == nil {
				expectedResult = nil
			}

			jobReportsServer.GetSchedulingReportResponse = expectedResult
			jobReportsServer.Err = tc.err
			jobReportsClient.GetSchedulingReportResponse = expectedResult
			jobReportsClient.Err = tc.err

			result, err := sut.GetSchedulingReport(ctx, request)

			assert.Equal(t, tc.err, err)
			assert.Equal(t, expectedResult, result)
			assert.Len(t, jobReportsServer.GetSchedulingReportCalls, tc.expectedNumReportServerCalls)
			assert.Len(t, jobReportsClient.GetSchedulingReportCalls, tc.expectedNumReportClientCalls)
		})
	}
}

func TestLeaderProxyingSchedulingReportsServer_GetQueueReport(t *testing.T) {
	tests := map[string]struct {
		err                          error
		isCurrentProcessLeader       bool
		expectedNumReportServerCalls int
		expectedNumReportClientCalls int
	}{
		// Should send all requests to local reports server when leader
		"current process leader": {
			err:                          nil,
			isCurrentProcessLeader:       true,
			expectedNumReportServerCalls: 1,
			expectedNumReportClientCalls: 0,
		},
		"current process leader return error": {
			err:                          fmt.Errorf("error"),
			isCurrentProcessLeader:       true,
			expectedNumReportServerCalls: 1,
			expectedNumReportClientCalls: 0,
		},
		// Should send all requests to remote server when not leader
		"remote process is leader": {
			err:                          nil,
			isCurrentProcessLeader:       false,
			expectedNumReportServerCalls: 0,
			expectedNumReportClientCalls: 1,
		},
		"remote process is leader return error": {
			err:                          fmt.Errorf("error"),
			isCurrentProcessLeader:       false,
			expectedNumReportServerCalls: 0,
			expectedNumReportClientCalls: 1,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			sut, clientProvider, jobReportsServer, jobReportsClient := setupLeaderProxyingSchedulerReportsServerTest(t)
			clientProvider.IsCurrentProcessLeader = tc.isCurrentProcessLeader

			request := &schedulerobjects.QueueReportRequest{Verbosity: int32(1), QueueName: "queue-1"}

			expectedResult := &schedulerobjects.QueueReport{Report: "report"}

			if tc.err == nil {
				expectedResult = nil
			}

			jobReportsServer.GetQueueReportResponse = expectedResult
			jobReportsServer.Err = tc.err
			jobReportsClient.GetQueueReportResponse = expectedResult
			jobReportsClient.Err = tc.err

			result, err := sut.GetQueueReport(ctx, request)

			assert.Equal(t, tc.err, err)
			assert.Equal(t, expectedResult, result)
			assert.Len(t, jobReportsServer.GetQueueReportCalls, tc.expectedNumReportServerCalls)
			assert.Len(t, jobReportsClient.GetQueueReportCalls, tc.expectedNumReportClientCalls)
		})
	}
}

func setupLeaderProxyingSchedulerReportsServerTest(t *testing.T) (*LeaderProxyingSchedulingReportsServer, *FakeClientProvider, *FakeSchedulerReportingServer, *FakeSchedulerReportingClient) {
	jobReportsServer := NewFakeSchedulerReportingServer()
	jobReportsClient := NewFakeSchedulerReportingClient()
	clientProvider := NewFakeClientProvider()

	reportsServer := NewLeaderProxyingSchedulingReportsServer(jobReportsServer, clientProvider)

	schedulingReportsClientProvider := NewFakeSchedulerReportingClientProvider()
	schedulingReportsClientProvider.Client = jobReportsClient
	reportsServer.schedulerReportingClientProvider = schedulingReportsClientProvider

	return reportsServer, clientProvider, jobReportsServer, jobReportsClient
}

type GetSchedulingReportCall struct {
	Context gocontext.Context
	Request *schedulerobjects.SchedulingReportRequest
}

type GetQueueReportCall struct {
	Context gocontext.Context
	Request *schedulerobjects.QueueReportRequest
}

type GetJobReportCall struct {
	Context gocontext.Context
	Request *schedulerobjects.JobReportRequest
}

type FakeSchedulerReportingServer struct {
	GetSchedulingReportCalls    []GetSchedulingReportCall
	GetSchedulingReportResponse *schedulerobjects.SchedulingReport

	GetQueueReportCalls    []GetQueueReportCall
	GetQueueReportResponse *schedulerobjects.QueueReport

	GetJobReportCalls    []GetJobReportCall
	GetJobReportResponse *schedulerobjects.JobReport
	Err                  error
}

func NewFakeSchedulerReportingServer() *FakeSchedulerReportingServer {
	return &FakeSchedulerReportingServer{
		GetSchedulingReportCalls: []GetSchedulingReportCall{},
		GetQueueReportCalls:      []GetQueueReportCall{},
		GetJobReportCalls:        []GetJobReportCall{},
	}
}

func (f *FakeSchedulerReportingServer) GetSchedulingReport(ctx gocontext.Context, request *schedulerobjects.SchedulingReportRequest) (*schedulerobjects.SchedulingReport, error) {
	f.GetSchedulingReportCalls = append(f.GetSchedulingReportCalls, GetSchedulingReportCall{Context: ctx, Request: request})
	return f.GetSchedulingReportResponse, f.Err
}

func (f *FakeSchedulerReportingServer) GetQueueReport(ctx gocontext.Context, request *schedulerobjects.QueueReportRequest) (*schedulerobjects.QueueReport, error) {
	f.GetQueueReportCalls = append(f.GetQueueReportCalls, GetQueueReportCall{Context: ctx, Request: request})
	return f.GetQueueReportResponse, f.Err
}

func (f *FakeSchedulerReportingServer) GetJobReport(ctx gocontext.Context, request *schedulerobjects.JobReportRequest) (*schedulerobjects.JobReport, error) {
	f.GetJobReportCalls = append(f.GetJobReportCalls, GetJobReportCall{Context: ctx, Request: request})
	return f.GetJobReportResponse, f.Err
}

type FakeSchedulerReportingClient struct {
	GetSchedulingReportCalls    []GetSchedulingReportCall
	GetSchedulingReportResponse *schedulerobjects.SchedulingReport

	GetQueueReportCalls    []GetQueueReportCall
	GetQueueReportResponse *schedulerobjects.QueueReport

	GetJobReportCalls    []GetJobReportCall
	GetJobReportResponse *schedulerobjects.JobReport
	Err                  error
}

func NewFakeSchedulerReportingClient() *FakeSchedulerReportingClient {
	return &FakeSchedulerReportingClient{
		GetSchedulingReportCalls: []GetSchedulingReportCall{},
		GetQueueReportCalls:      []GetQueueReportCall{},
		GetJobReportCalls:        []GetJobReportCall{},
	}
}

func (f *FakeSchedulerReportingClient) GetSchedulingReport(ctx gocontext.Context, request *schedulerobjects.SchedulingReportRequest, opts ...grpc.CallOption) (*schedulerobjects.SchedulingReport, error) {
	f.GetSchedulingReportCalls = append(f.GetSchedulingReportCalls, GetSchedulingReportCall{Context: ctx, Request: request})
	return f.GetSchedulingReportResponse, f.Err
}

func (f *FakeSchedulerReportingClient) GetQueueReport(ctx gocontext.Context, request *schedulerobjects.QueueReportRequest, opts ...grpc.CallOption) (*schedulerobjects.QueueReport, error) {
	f.GetQueueReportCalls = append(f.GetQueueReportCalls, GetQueueReportCall{Context: ctx, Request: request})
	return f.GetQueueReportResponse, f.Err
}

func (f *FakeSchedulerReportingClient) GetJobReport(ctx gocontext.Context, request *schedulerobjects.JobReportRequest, opts ...grpc.CallOption) (*schedulerobjects.JobReport, error) {
	f.GetJobReportCalls = append(f.GetJobReportCalls, GetJobReportCall{Context: ctx, Request: request})
	return f.GetJobReportResponse, f.Err
}

type FakeClientProvider struct {
	Error                  error
	IsCurrentProcessLeader bool
}

func NewFakeClientProvider() *FakeClientProvider {
	return &FakeClientProvider{}
}

func (f *FakeClientProvider) GetCurrentLeaderClientConnection() (bool, *grpc.ClientConn, error) {
	return f.IsCurrentProcessLeader, &grpc.ClientConn{}, f.Error
}

type FakeSchedulerReportingClientProvider struct {
	Client schedulerobjects.SchedulerReportingClient
}

func NewFakeSchedulerReportingClientProvider() *FakeSchedulerReportingClientProvider {
	return &FakeSchedulerReportingClientProvider{}
}

func (f *FakeSchedulerReportingClientProvider) GetSchedulerReportingClient(conn *grpc.ClientConn) schedulerobjects.SchedulerReportingClient {
	return f.Client
}
