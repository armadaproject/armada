package submit

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/clock"
	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/armada/mocks"
	"github.com/armadaproject/armada/internal/armada/permissions"
	"github.com/armadaproject/armada/internal/armada/submit/testfixtures"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/client/queue"
)

type mockObjects struct {
	publisher    *mocks.MockPublisher
	queueRepo    *mocks.MockQueueRepository
	jobRep       *mocks.MockJobRepository
	deduplicator *mocks.MockDeduplicator
	authorizer   *mocks.MockActionAuthorizer
}

func createMocks(t *testing.T) *mockObjects {
	ctrl := gomock.NewController(t)
	return &mockObjects{
		publisher:    mocks.NewMockPublisher(ctrl),
		queueRepo:    mocks.NewMockQueueRepository(ctrl),
		jobRep:       mocks.NewMockJobRepository(ctrl),
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

			mockedObjects.jobRep.
				EXPECT().
				StorePulsarSchedulerJobDetails(ctx, gomock.Any()).
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

func TestSubmit_SubmitCheckFailed(t *testing.T) {
	tests := map[string]struct {
		req *api.JobSubmitRequest
	}{
		"Submit check fails": {
			req: testfixtures.SubmitRequestWithNItems(1),
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
			server, mockedObjects := createTestServer(t)

			mockedObjects.queueRepo.
				EXPECT().GetQueue(ctx, tc.req.Queue).
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
				Return(nil, nil).
				Times(1)

			resp, err := server.SubmitJobs(ctx, tc.req)
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
		m.publisher,
		m.queueRepo,
		m.jobRep,
		testfixtures.DefaultSubmissionConfig(),
		m.deduplicator,
		m.authorizer)
	server.clock = clock.NewFakeClock(testfixtures.DefaultTime)
	server.idGenerator = testfixtures.TestUlidGenerator()
	return server, m
}
