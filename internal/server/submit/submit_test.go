package submit

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	clock "k8s.io/utils/clock/testing"
	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/auth/permission"
	commonMocks "github.com/armadaproject/armada/internal/common/mocks"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/server/mocks"
	"github.com/armadaproject/armada/internal/server/permissions"
	"github.com/armadaproject/armada/internal/server/submit/testfixtures"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/client/queue"
)

type mockObjects struct {
	publisher    *commonMocks.MockPublisher
	queueRepo    *mocks.MockQueueRepository
	deduplicator *mocks.MockDeduplicator
	authorizer   *mocks.MockActionAuthorizer
}

func createMocks(t *testing.T) *mockObjects {
	ctrl := gomock.NewController(t)
	return &mockObjects{
		publisher:    commonMocks.NewMockPublisher(ctrl),
		queueRepo:    mocks.NewMockQueueRepository(ctrl),
		deduplicator: mocks.NewMockDeduplicator(ctrl),
		authorizer:   mocks.NewMockActionAuthorizer(ctrl),
	}
}

func TestSubmit_Success(t *testing.T) {
	tests := map[string]struct {
		req              *api.JobSubmitRequest
		deduplicationIds map[string]string
		expectedEvents   []*armadaevents.EventSequence_Event
	}{
		"Submit request with one job": {
			req:            testfixtures.SubmitRequestWithNItems(1),
			expectedEvents: testfixtures.NEventSequenceEvents(1),
		},
		"Submit request with two jobs": {
			req:            testfixtures.SubmitRequestWithNItems(2),
			expectedEvents: testfixtures.NEventSequenceEvents(2),
		},
		"Submit request with two jobs, second is a duplicate": {
			req:              testfixtures.SubmitRequestWithNItems(2),
			deduplicationIds: map[string]string{"2": "3"},
			expectedEvents:   testfixtures.NEventSequenceEvents(1),
		},
		"Submit request without active deadline seconds": {
			req:            withActiveDeadlineSeconds(testfixtures.SubmitRequestWithNItems(1), nil),
			expectedEvents: testfixtures.NEventSequenceEvents(1),
		},
		"Submit request without termination grace period": {
			req:            withTerminationGracePeriod(testfixtures.SubmitRequestWithNItems(1), nil),
			expectedEvents: testfixtures.NEventSequenceEvents(1),
		},
		"Submit request without priority class": {
			req:            withPriorityClass(testfixtures.SubmitRequestWithNItems(1), ""),
			expectedEvents: testfixtures.NEventSequenceEvents(1),
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
			ctx = armadacontext.WithValue(ctx, "principal", testfixtures.DefaultPrincipal)

			server, mockedObjects := createTestServer(t)

			mockedObjects.queueRepo.
				EXPECT().
				GetQueue(ctx, tc.req.Queue).
				Return(testfixtures.DefaultQueue, nil).
				Times(1)

			mockedObjects.authorizer.
				EXPECT().
				AuthorizeQueueAction(ctx, testfixtures.DefaultQueue, permissions.SubmitAnyJobs, queue.PermissionVerbSubmit).
				Return(nil).
				Times(1)

			mockedObjects.deduplicator.
				EXPECT().
				GetOriginalJobIds(ctx, testfixtures.DefaultQueue.Name, tc.req.JobRequestItems).
				Return(tc.deduplicationIds, nil).
				Times(1)

			mockedObjects.deduplicator.
				EXPECT().
				StoreOriginalJobIds(ctx, testfixtures.DefaultQueue.Name, gomock.Any()).
				Times(1)

			expectedEventSequence := &armadaevents.EventSequence{
				Queue:      testfixtures.DefaultQueue.Name,
				JobSetName: testfixtures.DefaultJobset,
				UserId:     testfixtures.DefaultOwner,
				Groups:     []string{"everyone", "groupA"},
				Events:     tc.expectedEvents,
			}

			var capturedEventSequence *armadaevents.EventSequence
			mockedObjects.publisher.EXPECT().
				PublishMessages(ctx, gomock.Any()).
				Times(1).
				Do(func(_ interface{}, es *armadaevents.EventSequence) {
					capturedEventSequence = es
				})

			resp, err := server.SubmitJobs(ctx, tc.req)
			assert.NoError(t, err)
			assert.Equal(t, len(tc.req.JobRequestItems), len(resp.JobResponseItems))
			assert.Equal(t, expectedEventSequence, capturedEventSequence)
			cancel()
		})
	}
}

func TestSubmit_FailedValidation(t *testing.T) {
	tests := map[string]struct {
		req *api.JobSubmitRequest
	}{
		"No Podspec": {
			req: withPodSpec(testfixtures.SubmitRequestWithNItems(1), nil),
		},
		"InvalidAffinity": {
			req: withAffinity(testfixtures.SubmitRequestWithNItems(1), &v1.Affinity{
				NodeAffinity: &v1.NodeAffinity{
					PreferredDuringSchedulingIgnoredDuringExecution: []v1.PreferredSchedulingTerm{
						{},
					},
				},
			}),
		},
		"No Namespace": {
			req: withNamespace(testfixtures.SubmitRequestWithNItems(1), ""),
		},
		"No Queue": {
			req: withQueue(testfixtures.SubmitRequestWithNItems(1), ""),
		},
		"Invalid Priority Class": {
			req: withPriorityClass(testfixtures.SubmitRequestWithNItems(1), "invalidPc"),
		},
		"Below Minimum Termination Grace Period": {
			req: withTerminationGracePeriod(testfixtures.SubmitRequestWithNItems(1), pointer.Int64(1)),
		},
		"Above Maximum Termination Grace Period": {
			req: withTerminationGracePeriod(testfixtures.SubmitRequestWithNItems(1), pointer.Int64(1000000)),
		},
		"Requests not equal to Limits": {
			req: withResources(testfixtures.SubmitRequestWithNItems(1), v1.ResourceRequirements{
				Limits:   v1.ResourceList{"cpu": resource.MustParse("1")},
				Requests: v1.ResourceList{"cpu": resource.MustParse("2")},
			}),
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
			server, mockedObjects := createTestServer(t)

			mockedObjects.queueRepo.
				EXPECT().
				GetQueue(ctx, tc.req.Queue).
				Return(testfixtures.DefaultQueue, nil).
				Times(1)

			mockedObjects.authorizer.
				EXPECT().
				AuthorizeQueueAction(ctx, testfixtures.DefaultQueue, permissions.SubmitAnyJobs, queue.PermissionVerbSubmit).
				Return(nil).
				Times(1)

			resp, err := server.SubmitJobs(ctx, tc.req)
			assert.Error(t, err)
			assert.Nil(t, resp)
			cancel()
		})
	}
}

func TestCancelJobs(t *testing.T) {
	jobId1 := util.ULID().String()
	jobId2 := util.ULID().String()
	tests := map[string]struct {
		req            *api.JobCancelRequest
		expectedEvents []*armadaevents.EventSequence_Event
	}{
		"Cancel job using JobId": {
			req:            &api.JobCancelRequest{JobId: jobId1, Queue: testfixtures.DefaultQueue.Name, JobSetId: testfixtures.DefaultJobset},
			expectedEvents: testfixtures.CreateCancelJobSequenceEvents([]string{jobId1}),
		},
		"Cancel jobs using JobIds": {
			req:            &api.JobCancelRequest{JobIds: []string{jobId1, jobId2}, Queue: testfixtures.DefaultQueue.Name, JobSetId: testfixtures.DefaultJobset},
			expectedEvents: testfixtures.CreateCancelJobSequenceEvents([]string{jobId1, jobId2}),
		},
		"Cancel jobs using both JobId and JobIds": {
			req:            &api.JobCancelRequest{JobId: jobId1, JobIds: []string{jobId2}, Queue: testfixtures.DefaultQueue.Name, JobSetId: testfixtures.DefaultJobset},
			expectedEvents: testfixtures.CreateCancelJobSequenceEvents([]string{jobId2, jobId1}),
		},
		"Cancel jobs using both JobId and JobIds - overlapping ids": {
			req:            &api.JobCancelRequest{JobId: jobId1, JobIds: []string{jobId1}, Queue: testfixtures.DefaultQueue.Name, JobSetId: testfixtures.DefaultJobset},
			expectedEvents: testfixtures.CreateCancelJobSequenceEvents([]string{jobId1}),
		},
		"Cancel jobSet": {
			req:            &api.JobCancelRequest{Queue: testfixtures.DefaultQueue.Name, JobSetId: testfixtures.DefaultJobset},
			expectedEvents: []*armadaevents.EventSequence_Event{testfixtures.CreateCancelJobSetSequenceEvent()},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
			ctx = armadacontext.WithValue(ctx, "principal", testfixtures.DefaultPrincipal)

			server, mockedObjects := createTestServer(t)

			mockedObjects.queueRepo.
				EXPECT().
				GetQueue(ctx, tc.req.Queue).
				Return(testfixtures.DefaultQueue, nil).
				Times(1)

			mockedObjects.authorizer.
				EXPECT().
				AuthorizeQueueAction(ctx, testfixtures.DefaultQueue, permission.Permission(permissions.CancelAnyJobs), queue.PermissionVerbCancel).
				Return(nil).
				Times(1)

			expectedEventSequence := &armadaevents.EventSequence{
				Queue:      testfixtures.DefaultQueue.Name,
				JobSetName: testfixtures.DefaultJobset,
				UserId:     testfixtures.DefaultOwner,
				Groups:     []string{"everyone", "groupA"},
				Events:     tc.expectedEvents,
			}

			var capturedEventSequence *armadaevents.EventSequence
			mockedObjects.publisher.EXPECT().
				PublishMessages(ctx, gomock.Any()).
				Times(1).
				Do(func(_ interface{}, es *armadaevents.EventSequence) {
					capturedEventSequence = es
				})

			_, err := server.CancelJobs(ctx, tc.req)
			assert.NoError(t, err)
			assert.Equal(t, expectedEventSequence, capturedEventSequence)
			cancel()
		})
	}
}

func TestCancelJobs_FailedValidation(t *testing.T) {
	jobId1 := util.ULID().String()
	tests := map[string]struct {
		req *api.JobCancelRequest
	}{
		"Queue is empty": {
			req: &api.JobCancelRequest{JobId: jobId1, JobSetId: testfixtures.DefaultJobset},
		},
		"Job set is empty": {
			req: &api.JobCancelRequest{JobId: jobId1, Queue: testfixtures.DefaultQueue.Name},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
			server, _ := createTestServer(t)

			resp, err := server.CancelJobs(ctx, tc.req)
			assert.Error(t, err)
			assert.Nil(t, resp)
			cancel()
		})
	}
}

func TestPreemptJobs(t *testing.T) {
	jobId1 := util.NewULID()
	jobId2 := util.NewULID()
	tests := map[string]struct {
		req            *api.JobPreemptRequest
		expectedEvents []*armadaevents.EventSequence_Event
	}{
		"Preempt jobs using JobIds": {
			req:            &api.JobPreemptRequest{JobIds: []string{jobId1, jobId2}, Queue: testfixtures.DefaultQueue.Name, JobSetId: testfixtures.DefaultJobset},
			expectedEvents: testfixtures.CreatePreemptJobSequenceEvents([]string{jobId1, jobId2}),
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
			ctx = armadacontext.WithValue(ctx, "principal", testfixtures.DefaultPrincipal)

			server, mockedObjects := createTestServer(t)

			mockedObjects.queueRepo.
				EXPECT().
				GetQueue(ctx, tc.req.Queue).
				Return(testfixtures.DefaultQueue, nil).
				Times(1)

			mockedObjects.authorizer.
				EXPECT().
				AuthorizeQueueAction(ctx, testfixtures.DefaultQueue, permission.Permission(permissions.PreemptAnyJobs), queue.PermissionVerbPreempt).
				Return(nil).
				Times(1)

			expectedEventSequence := &armadaevents.EventSequence{
				Queue:      testfixtures.DefaultQueue.Name,
				JobSetName: testfixtures.DefaultJobset,
				UserId:     testfixtures.DefaultOwner,
				Groups:     []string{"everyone", "groupA"},
				Events:     tc.expectedEvents,
			}

			var capturedEventSequence *armadaevents.EventSequence
			mockedObjects.publisher.EXPECT().
				PublishMessages(ctx, gomock.Any()).
				Times(1).
				Do(func(_ interface{}, es *armadaevents.EventSequence) {
					capturedEventSequence = es
				})

			_, err := server.PreemptJobs(ctx, tc.req)
			assert.NoError(t, err)
			assert.Equal(t, expectedEventSequence, capturedEventSequence)
			cancel()
		})
	}
}

func TestPreemptJobs_FailedValidation(t *testing.T) {
	jobId1 := util.ULID().String()
	tests := map[string]struct {
		req *api.JobPreemptRequest
	}{
		"Queue is empty": {
			req: &api.JobPreemptRequest{JobIds: []string{jobId1}, JobSetId: testfixtures.DefaultJobset},
		},
		"Job set is empty": {
			req: &api.JobPreemptRequest{JobIds: []string{jobId1}, Queue: testfixtures.DefaultQueue.Name},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
			server, _ := createTestServer(t)

			resp, err := server.PreemptJobs(ctx, tc.req)
			assert.Error(t, err)
			assert.Nil(t, resp)
			cancel()
		})
	}
}

func TestReprioritizeJobs(t *testing.T) {
	jobId1 := util.ULID().String()
	jobId2 := util.ULID().String()
	newPriority := float64(5)
	tests := map[string]struct {
		req            *api.JobReprioritizeRequest
		expectedEvents []*armadaevents.EventSequence_Event
	}{
		"Reprioritize jobs using JobIds": {
			req:            &api.JobReprioritizeRequest{JobIds: []string{jobId1, jobId2}, Queue: testfixtures.DefaultQueue.Name, JobSetId: testfixtures.DefaultJobset, NewPriority: newPriority},
			expectedEvents: testfixtures.CreateReprioritizeJobSequenceEvents([]string{jobId1, jobId2}, newPriority),
		},
		"Reprioritize jobSet": {
			req:            &api.JobReprioritizeRequest{Queue: testfixtures.DefaultQueue.Name, JobSetId: testfixtures.DefaultJobset, NewPriority: newPriority},
			expectedEvents: []*armadaevents.EventSequence_Event{testfixtures.CreateReprioritizedJobSetSequenceEvent(newPriority)},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
			ctx = armadacontext.WithValue(ctx, "principal", testfixtures.DefaultPrincipal)

			server, mockedObjects := createTestServer(t)

			mockedObjects.queueRepo.
				EXPECT().
				GetQueue(ctx, tc.req.Queue).
				Return(testfixtures.DefaultQueue, nil).
				Times(1)

			mockedObjects.authorizer.
				EXPECT().
				AuthorizeQueueAction(ctx, testfixtures.DefaultQueue, permission.Permission(permissions.ReprioritizeAnyJobs), queue.PermissionVerbReprioritize).
				Return(nil).
				Times(1)

			expectedEventSequence := &armadaevents.EventSequence{
				Queue:      testfixtures.DefaultQueue.Name,
				JobSetName: testfixtures.DefaultJobset,
				UserId:     testfixtures.DefaultOwner,
				Groups:     []string{"everyone", "groupA"},
				Events:     tc.expectedEvents,
			}

			var capturedEventSequence *armadaevents.EventSequence
			mockedObjects.publisher.EXPECT().
				PublishMessages(ctx, gomock.Any()).
				Times(1).
				Do(func(_ interface{}, es *armadaevents.EventSequence) {
					capturedEventSequence = es
				})

			_, err := server.ReprioritizeJobs(ctx, tc.req)
			assert.NoError(t, err)
			assert.Equal(t, expectedEventSequence, capturedEventSequence)
			cancel()
		})
	}
}

func TestReprioritizeJobs_FailedValidation(t *testing.T) {
	jobId1 := util.ULID().String()
	tests := map[string]struct {
		req *api.JobReprioritizeRequest
	}{
		"Queue is empty": {
			req: &api.JobReprioritizeRequest{JobIds: []string{jobId1}, JobSetId: testfixtures.DefaultJobset},
		},
		"Job set is empty": {
			req: &api.JobReprioritizeRequest{JobIds: []string{jobId1}, Queue: testfixtures.DefaultQueue.Name},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
			server, _ := createTestServer(t)

			resp, err := server.ReprioritizeJobs(ctx, tc.req)
			assert.Error(t, err)
			assert.Nil(t, resp)
			cancel()
		})
	}
}

func withNamespace(req *api.JobSubmitRequest, n string) *api.JobSubmitRequest {
	for _, item := range req.JobRequestItems {
		item.Namespace = n
	}
	return req
}

func withQueue(req *api.JobSubmitRequest, q string) *api.JobSubmitRequest {
	req.Queue = q
	return req
}

func withResources(req *api.JobSubmitRequest, r v1.ResourceRequirements) *api.JobSubmitRequest {
	for _, item := range req.JobRequestItems {
		containers := make([]v1.Container, len(item.PodSpec.Containers))
		for i, container := range item.PodSpec.Containers {
			container.Resources = r
			containers[i] = container
		}
		item.PodSpec.Containers = containers
	}
	return req
}

func withActiveDeadlineSeconds(req *api.JobSubmitRequest, v *int64) *api.JobSubmitRequest {
	for _, item := range req.JobRequestItems {
		item.PodSpec.ActiveDeadlineSeconds = v
	}
	return req
}

func withPodSpec(req *api.JobSubmitRequest, p *v1.PodSpec) *api.JobSubmitRequest {
	for _, item := range req.JobRequestItems {
		item.PodSpec = p
	}
	return req
}

func withAffinity(req *api.JobSubmitRequest, a *v1.Affinity) *api.JobSubmitRequest {
	for _, item := range req.JobRequestItems {
		item.PodSpec.Affinity = a
	}
	return req
}

func withPriorityClass(req *api.JobSubmitRequest, pc string) *api.JobSubmitRequest {
	for _, item := range req.JobRequestItems {
		item.PodSpec.PriorityClassName = pc
	}
	return req
}

func withTerminationGracePeriod(req *api.JobSubmitRequest, v *int64) *api.JobSubmitRequest {
	for _, item := range req.JobRequestItems {
		item.PodSpec.TerminationGracePeriodSeconds = v
	}
	return req
}

func createTestServer(t *testing.T) (*Server, *mockObjects) {
	m := createMocks(t)
	server := NewServer(
		nil,
		m.publisher,
		m.queueRepo,
		testfixtures.DefaultSubmissionConfig(),
		m.deduplicator,
		m.authorizer)
	server.clock = clock.NewFakeClock(testfixtures.DefaultTime)
	server.idGenerator = testfixtures.TestUlidGenerator()
	return server, m
}
