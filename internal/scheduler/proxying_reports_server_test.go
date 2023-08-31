package scheduler

import (
	"fmt"
	"github.com/armadaproject/armada/internal/common/context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

func TestProxyingSchedulingReportsServer_GetJobReports(t *testing.T) {
	tests := map[string]struct {
		err error
	}{
		"no error": {
			err: nil,
		},
		"on error": {
			err: fmt.Errorf("error"),
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			sut, jobReportsClient := setupProxyingSchedulerReportsServerTest(t)

			request := &schedulerobjects.JobReportRequest{JobId: "job-1"}

			expectedResult := &schedulerobjects.JobReport{Report: "report"}

			if tc.err == nil {
				expectedResult = nil
			}

			jobReportsClient.GetJobReportResponse = expectedResult
			jobReportsClient.Err = tc.err

			result, err := sut.GetJobReport(ctx, request)

			assert.Equal(t, tc.err, err)
			assert.Equal(t, expectedResult, result)
			assert.Len(t, jobReportsClient.GetJobReportCalls, 1)
		})
	}
}

func TestProxyingSchedulingReportsServer_GetSchedulingReport(t *testing.T) {
	tests := map[string]struct {
		err error
	}{
		"no error": {
			err: nil,
		},
		"on error": {
			err: fmt.Errorf("error"),
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			sut, jobReportsClient := setupProxyingSchedulerReportsServerTest(t)

			request := &schedulerobjects.SchedulingReportRequest{Verbosity: int32(1)}

			expectedResult := &schedulerobjects.SchedulingReport{Report: "report"}

			if tc.err == nil {
				expectedResult = nil
			}

			jobReportsClient.GetSchedulingReportResponse = expectedResult
			jobReportsClient.Err = tc.err

			result, err := sut.GetSchedulingReport(ctx, request)

			assert.Equal(t, tc.err, err)
			assert.Equal(t, expectedResult, result)
			assert.Len(t, jobReportsClient.GetSchedulingReportCalls, 1)
		})
	}
}

func TestProxyingSchedulingReportsServer_GetQueueReport(t *testing.T) {
	tests := map[string]struct {
		err error
	}{
		"no error": {
			err: nil,
		},
		"on error": {
			err: fmt.Errorf("error"),
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			sut, jobReportsClient := setupProxyingSchedulerReportsServerTest(t)

			request := &schedulerobjects.QueueReportRequest{Verbosity: int32(1), QueueName: "queue-1"}

			expectedResult := &schedulerobjects.QueueReport{Report: "report"}

			if tc.err == nil {
				expectedResult = nil
			}

			jobReportsClient.GetQueueReportResponse = expectedResult
			jobReportsClient.Err = tc.err

			result, err := sut.GetQueueReport(ctx, request)

			assert.Equal(t, tc.err, err)
			assert.Equal(t, expectedResult, result)
			assert.Len(t, jobReportsClient.GetQueueReportCalls, 1)
		})
	}
}

func setupProxyingSchedulerReportsServerTest(t *testing.T) (*ProxyingSchedulingReportsServer, *FakeSchedulerReportingClient) {
	schedulerReportsClient := NewFakeSchedulerReportingClient()
	sut := NewProxyingSchedulingReportsServer(schedulerReportsClient)
	return sut, schedulerReportsClient
}
