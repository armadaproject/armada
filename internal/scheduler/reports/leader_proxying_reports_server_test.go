package reports

import (
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.com/armadaproject/armada/internal/common/armadacontext"
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
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
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
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
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
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
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

func TestLeaderProxyingSchedulingReportsServer_GetExecutors(t *testing.T) {
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
			_, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
			defer cancel()

			sut, clientProvider, jobReportsServer, jobReportsClient := setupLeaderProxyingSchedulerReportsServerTest(t)
			getExecutorsServer := NewFakeSchedulerReporting_GetExecutorsServer()
			clientProvider.IsCurrentProcessLeader = tc.isCurrentProcessLeader
			messages := []*schedulerobjects.StreamingExecutorMessage{
				{
					Event: &schedulerobjects.StreamingExecutorMessage_Executor{
						Executor: nil,
					},
				},
				{
					Event: &schedulerobjects.StreamingExecutorMessage_End{End: nil},
				},
			}
			jobReportsClient.GetExecutorsResponse = NewFakeSchedulerReporting_GetExecutorsClient(messages)
			jobReportsServer.GetExecutorsResponse = messages

			request := &schedulerobjects.StreamingExecutorGetRequest{Num: 0}

			jobReportsServer.Err = tc.err
			jobReportsClient.Err = tc.err

			err := sut.GetExecutors(request, getExecutorsServer)

			if tc.err == nil {
				assert.Equal(t, messages, getExecutorsServer.Sent)
			}

			assert.Equal(t, tc.err, err)
			assert.Len(t, jobReportsServer.GetExecutorsCalls, tc.expectedNumReportServerCalls)
			assert.Len(t, jobReportsClient.GetExecutorsCalls, tc.expectedNumReportClientCalls)
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
	Context context.Context
	Request *schedulerobjects.SchedulingReportRequest
}

type GetQueueReportCall struct {
	Context context.Context
	Request *schedulerobjects.QueueReportRequest
}

type GetJobReportCall struct {
	Context context.Context
	Request *schedulerobjects.JobReportRequest
}

type GetExecutorsCall struct {
	Context context.Context
	Request *schedulerobjects.StreamingExecutorGetRequest
}

type FakeSchedulerReportingServer struct {
	GetSchedulingReportCalls    []GetSchedulingReportCall
	GetSchedulingReportResponse *schedulerobjects.SchedulingReport

	GetQueueReportCalls    []GetQueueReportCall
	GetQueueReportResponse *schedulerobjects.QueueReport

	GetJobReportCalls    []GetJobReportCall
	GetJobReportResponse *schedulerobjects.JobReport

	GetExecutorsCalls    []GetExecutorsCall
	GetExecutorsResponse []*schedulerobjects.StreamingExecutorMessage
	Err                  error
}

func NewFakeSchedulerReportingServer() *FakeSchedulerReportingServer {
	return &FakeSchedulerReportingServer{
		GetSchedulingReportCalls: []GetSchedulingReportCall{},
		GetQueueReportCalls:      []GetQueueReportCall{},
		GetJobReportCalls:        []GetJobReportCall{},
		GetExecutorsCalls:        []GetExecutorsCall{},
	}
}

func (f *FakeSchedulerReportingServer) GetSchedulingReport(ctx context.Context, request *schedulerobjects.SchedulingReportRequest) (*schedulerobjects.SchedulingReport, error) {
	f.GetSchedulingReportCalls = append(f.GetSchedulingReportCalls, GetSchedulingReportCall{Context: ctx, Request: request})
	return f.GetSchedulingReportResponse, f.Err
}

func (f *FakeSchedulerReportingServer) GetQueueReport(ctx context.Context, request *schedulerobjects.QueueReportRequest) (*schedulerobjects.QueueReport, error) {
	f.GetQueueReportCalls = append(f.GetQueueReportCalls, GetQueueReportCall{Context: ctx, Request: request})
	return f.GetQueueReportResponse, f.Err
}

func (f *FakeSchedulerReportingServer) GetJobReport(ctx context.Context, request *schedulerobjects.JobReportRequest) (*schedulerobjects.JobReport, error) {
	f.GetJobReportCalls = append(f.GetJobReportCalls, GetJobReportCall{Context: ctx, Request: request})
	return f.GetJobReportResponse, f.Err
}

func (f *FakeSchedulerReportingServer) GetExecutors(request *schedulerobjects.StreamingExecutorGetRequest, srv schedulerobjects.SchedulerReporting_GetExecutorsServer) error {
	f.GetExecutorsCalls = append(f.GetExecutorsCalls, GetExecutorsCall{Context: srv.Context(), Request: request})
	for _, msg := range f.GetExecutorsResponse {
		if err := srv.Send(msg); err != nil {
			return err
		}
	}
	return f.Err
}

type FakeSchedulerReporting_GetExecutorsServer struct {
	Sent []*schedulerobjects.StreamingExecutorMessage
}

func (f *FakeSchedulerReporting_GetExecutorsServer) SetHeader(md metadata.MD) error {
	return nil
}

func (f *FakeSchedulerReporting_GetExecutorsServer) SendHeader(md metadata.MD) error {
	return nil
}

func (f *FakeSchedulerReporting_GetExecutorsServer) SetTrailer(md metadata.MD) {
	return
}

func (f *FakeSchedulerReporting_GetExecutorsServer) Context() context.Context {
	return context.Background()
}

func (f *FakeSchedulerReporting_GetExecutorsServer) SendMsg(m any) error {
	return nil
}

func (f *FakeSchedulerReporting_GetExecutorsServer) RecvMsg(m any) error {
	return nil
}

func NewFakeSchedulerReporting_GetExecutorsServer() *FakeSchedulerReporting_GetExecutorsServer {
	return &FakeSchedulerReporting_GetExecutorsServer{
		Sent: []*schedulerobjects.StreamingExecutorMessage{},
	}
}

func (f *FakeSchedulerReporting_GetExecutorsServer) Send(s *schedulerobjects.StreamingExecutorMessage) error {
	f.Sent = append(f.Sent, s)
	return nil
}

type FakeSchedulerReporting_GetExecutorsClient struct {
	toReceive []*schedulerobjects.StreamingExecutorMessage
}

func (f *FakeSchedulerReporting_GetExecutorsClient) Recv() (*schedulerobjects.StreamingExecutorMessage, error) {
	if len(f.toReceive) > 0 {
		msg := f.toReceive[0]
		f.toReceive = f.toReceive[1:]
		return msg, nil
	}
	return nil, fmt.Errorf("No more items to receive")
}

func (f *FakeSchedulerReporting_GetExecutorsClient) Header() (metadata.MD, error) {
	return metadata.MD{}, nil
}

func (f *FakeSchedulerReporting_GetExecutorsClient) Trailer() metadata.MD {
	return metadata.MD{}
}

func (f *FakeSchedulerReporting_GetExecutorsClient) CloseSend() error {
	return nil
}

func (f *FakeSchedulerReporting_GetExecutorsClient) Context() context.Context {
	return context.Background()
}

func (f *FakeSchedulerReporting_GetExecutorsClient) SendMsg(m any) error {
	return nil
}

func (f *FakeSchedulerReporting_GetExecutorsClient) RecvMsg(m any) error {
	return nil
}

func NewFakeSchedulerReporting_GetExecutorsClient(toReceive []*schedulerobjects.StreamingExecutorMessage) *FakeSchedulerReporting_GetExecutorsClient {
	return &FakeSchedulerReporting_GetExecutorsClient{
		toReceive: toReceive,
	}
}

type FakeSchedulerReportingClient struct {
	GetSchedulingReportCalls    []GetSchedulingReportCall
	GetSchedulingReportResponse *schedulerobjects.SchedulingReport

	GetQueueReportCalls    []GetQueueReportCall
	GetQueueReportResponse *schedulerobjects.QueueReport

	GetJobReportCalls    []GetJobReportCall
	GetJobReportResponse *schedulerobjects.JobReport

	GetExecutorsCalls    []GetExecutorsCall
	GetExecutorsResponse schedulerobjects.SchedulerReporting_GetExecutorsClient
	Err                  error
}

func NewFakeSchedulerReportingClient() *FakeSchedulerReportingClient {
	return &FakeSchedulerReportingClient{
		GetSchedulingReportCalls: []GetSchedulingReportCall{},
		GetQueueReportCalls:      []GetQueueReportCall{},
		GetJobReportCalls:        []GetJobReportCall{},
		GetExecutorsCalls:        []GetExecutorsCall{},
	}
}

func (f *FakeSchedulerReportingClient) GetSchedulingReport(ctx context.Context, request *schedulerobjects.SchedulingReportRequest, opts ...grpc.CallOption) (*schedulerobjects.SchedulingReport, error) {
	f.GetSchedulingReportCalls = append(f.GetSchedulingReportCalls, GetSchedulingReportCall{Context: ctx, Request: request})
	return f.GetSchedulingReportResponse, f.Err
}

func (f *FakeSchedulerReportingClient) GetQueueReport(ctx context.Context, request *schedulerobjects.QueueReportRequest, opts ...grpc.CallOption) (*schedulerobjects.QueueReport, error) {
	f.GetQueueReportCalls = append(f.GetQueueReportCalls, GetQueueReportCall{Context: ctx, Request: request})
	return f.GetQueueReportResponse, f.Err
}

func (f *FakeSchedulerReportingClient) GetJobReport(ctx context.Context, request *schedulerobjects.JobReportRequest, opts ...grpc.CallOption) (*schedulerobjects.JobReport, error) {
	f.GetJobReportCalls = append(f.GetJobReportCalls, GetJobReportCall{Context: ctx, Request: request})
	return f.GetJobReportResponse, f.Err
}

func (f *FakeSchedulerReportingClient) GetExecutors(ctx context.Context, request *schedulerobjects.StreamingExecutorGetRequest, opts ...grpc.CallOption) (schedulerobjects.SchedulerReporting_GetExecutorsClient, error) {
	f.GetExecutorsCalls = append(f.GetExecutorsCalls, GetExecutorsCall{Context: ctx, Request: request})
	return f.GetExecutorsResponse, f.Err
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
	Client *FakeSchedulerReportingClient
}

func NewFakeSchedulerReportingClientProvider() *FakeSchedulerReportingClientProvider {
	return &FakeSchedulerReportingClientProvider{}
}

func (f *FakeSchedulerReportingClientProvider) GetSchedulerReportingClient(conn *grpc.ClientConn) schedulerobjects.SchedulerReportingClient {
	return f.Client
}
