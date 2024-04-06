package submit

import (
	"fmt"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"k8s.io/apimachinery/pkg/util/clock"
	"k8s.io/utils/pointer"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/armada/mocks"
	"github.com/armadaproject/armada/internal/armada/permissions"
	"github.com/armadaproject/armada/internal/common/auth/authorization"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/client/queue"
)

var (
	defaultNamespace = "testNamespace"
	defaultQueue     = queue.Queue{Name: "testQueue"}
	defaultPriority  = uint32(1000)
	defaultPrincipal = authorization.NewStaticPrincipal("testUser", []string{"groupA"})
	defaultResources = v1.ResourceRequirements{
		Requests: v1.ResourceList{
			"cpu":    resource.MustParse("1"),
			"memory": resource.MustParse("1Gi"),
		},
		Limits: v1.ResourceList{
			"cpu":    resource.MustParse("1"),
			"memory": resource.MustParse("1Gi"),
		},
	}
	defaultContainer   = []v1.Container{{Resources: defaultResources}}
	defaultTolerations = []v1.Toleration{
		{
			Key:      "armadaproject.io/foo",
			Operator: "Exists",
		},
	}
	defaultPriorityClass                 = "testPriorityClass"
	defaultTerminationGracePeriodSeconds = int64(30)
	defaultActiveDeadlineSeconds         = int64(3600)
	baseTime                             = time.Now().UTC()
)

func TestNeeded(t *testing.T) {

	// ***Success***
	// Single job
	// Two jobs
	// Duplicate job
	// Defaulted fields

	// ***Failure***
	// Single job fails validation
	// two jobs, one passes, one fails validation
	// unauthorized
	// cannot be scheduled
	// fail to publish

}

func TestSubmit_Success(t *testing.T) {

	tests := map[string]struct {
		req              *api.JobSubmitRequest
		deduplicationIds map[string]string
		expectedEvents   []*armadaevents.EventSequence_Event
	}{
		"Submit request with one job": {
			req:            submitRequestWithNItems(1),
			expectedEvents: nEventSequenceEvents(1),
		},
		"Submit request with two jobs": {
			req:            submitRequestWithNItems(2),
			expectedEvents: nEventSequenceEvents(2),
		},
		"Submit request with two jobs, second is a duplicate": {
			req:              submitRequestWithNItems(2),
			deduplicationIds: map[string]string{"2": "3"},
			expectedEvents:   nEventSequenceEvents(1),
		},
		"Submit request without active deadline seconds": {
			req:            withActiveDeadlineSeconds(submitRequestWithNItems(1), nil),
			expectedEvents: nEventSequenceEvents(1),
		},
		"Submit request without termination grace period": {
			req:            withTerminationGracePeriod(submitRequestWithNItems(1), nil),
			expectedEvents: nEventSequenceEvents(1),
		},
		"Submit request without priority class": {
			req:            withPriorityClass(submitRequestWithNItems(1), ""),
			expectedEvents: nEventSequenceEvents(1),
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {

			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
			ctx = armadacontext.WithValue(ctx, "principal", defaultPrincipal)

			expectedEventSequence := &armadaevents.EventSequence{
				Queue:      defaultQueue.Name,
				JobSetName: "testJobset",
				UserId:     "testUser",
				Groups:     []string{"everyone", "groupA"},
				Events:     tc.expectedEvents,
			}

			ctrl := gomock.NewController(t)
			publisher := mocks.NewMockPublisher(ctrl)
			queueRepository := mocks.NewMockQueueRepository(ctrl)
			jobRepository := mocks.NewMockJobRepository(ctrl)
			deduplicator := mocks.NewMockDeduplicator(ctrl)
			submitChecker := mocks.NewMockSubmitScheduleChecker(ctrl)
			authorizer := mocks.NewMockActionAuthorizer(ctrl)

			// Generates ulids starting at "00000000000000000000000001" and incrementing from there
			counter := 0
			testUlidGenerator := func() *armadaevents.Uuid {
				counter++
				return testUlid(counter)
			}

			server := NewServer(
				publisher,
				queueRepository,
				jobRepository,
				defaultSubmissionConfig(),
				deduplicator,
				submitChecker,
				authorizer)

			server.clock = clock.NewFakeClock(baseTime)
			server.idGenerator = testUlidGenerator

			queueRepository.EXPECT().GetQueue(ctx, tc.req.Queue).Return(defaultQueue, nil)
			authorizer.EXPECT().AuthorizeQueueAction(ctx, defaultQueue, permissions.SubmitAnyJobs, queue.PermissionVerbSubmit).Return(nil).Times(1)
			deduplicator.EXPECT().GetOriginalJobIds(ctx, defaultQueue.Name, tc.req.JobRequestItems).Return(tc.deduplicationIds, nil).Times(1)
			deduplicator.EXPECT().StoreOriginalJobIds(ctx, defaultQueue.Name, gomock.Any())
			submitChecker.EXPECT().CheckApiJobs(gomock.Any()).Return(true, "").Times(1)
			jobRepository.EXPECT().StorePulsarSchedulerJobDetails(ctx, gomock.Any()).Return(nil).Times(1)

			var capturedEventSequence *armadaevents.EventSequence
			publisher.EXPECT().
				PublishMessages(ctx, gomock.Any()).
				Times(1).
				Do(func(_ interface{}, es *armadaevents.EventSequence) {
					capturedEventSequence = es
				})

			resp, err := server.SubmitJobs(ctx, tc.req)
			assert.NoError(t, err)
			assert.Equal(t, len(tc.req.JobRequestItems), len(resp.JobResponseItems))
			a := expectedEventSequence
			b := capturedEventSequence
			assert.Equal(t, a, b)
			cancel()
		})
	}
}

func TestSubmit_FailedValidation(t *testing.T) {
	tests := map[string]struct {
		req *api.JobSubmitRequest
	}{
		"No Podspec": {
			req: withPodSpec(submitRequestWithNItems(1), nil),
		},
		"InvalidAffinity": {
			req: withAffinity(submitRequestWithNItems(1), &v1.Affinity{
				NodeAffinity: &v1.NodeAffinity{
					PreferredDuringSchedulingIgnoredDuringExecution: []v1.PreferredSchedulingTerm{
						{},
					},
				},
			}),
		},
		"No Namespace": {
			req: withNamespace(submitRequestWithNItems(1), ""),
		},
		"No Queue": {
			req: withQueue(submitRequestWithNItems(1), ""),
		},
		"Invalid Priority Class": {
			req: withPriorityClass(submitRequestWithNItems(1), "invalidPc"),
		},
		"Below Minimum Termination Grace Period": {
			req: withTerminationGracePeriod(submitRequestWithNItems(1), pointer.Int64(1)),
		},
		"Above Maximum Termination Grace Period": {
			req: withTerminationGracePeriod(submitRequestWithNItems(1), pointer.Int64(1000000)),
		},
		"Requests not equal to Limits": {
			req: withResources(submitRequestWithNItems(1), v1.ResourceRequirements{
				Limits:   v1.ResourceList{"cpu": resource.MustParse("1")},
				Requests: v1.ResourceList{"cpu": resource.MustParse("2")},
			}),
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {

			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)

			ctrl := gomock.NewController(t)
			publisher := mocks.NewMockPublisher(ctrl)
			queueRepository := mocks.NewMockQueueRepository(ctrl)
			jobRepository := mocks.NewMockJobRepository(ctrl)
			deduplicator := mocks.NewMockDeduplicator(ctrl)
			submitChecker := mocks.NewMockSubmitScheduleChecker(ctrl)
			authorizer := mocks.NewMockActionAuthorizer(ctrl)

			// Generates ulids starting at "00000000000000000000000001" and incrementing from there
			counter := 0
			testUlidGenerator := func() *armadaevents.Uuid {
				counter++
				return testUlid(counter)
			}

			server := NewServer(
				publisher,
				queueRepository,
				jobRepository,
				defaultSubmissionConfig(),
				deduplicator,
				submitChecker,
				authorizer)

			server.clock = clock.NewFakeClock(baseTime)
			server.idGenerator = testUlidGenerator

			queueRepository.EXPECT().GetQueue(ctx, tc.req.Queue).Return(defaultQueue, nil)
			authorizer.EXPECT().AuthorizeQueueAction(ctx, defaultQueue, permissions.SubmitAnyJobs, queue.PermissionVerbSubmit).Return(nil).Times(1)
			resp, err := server.SubmitJobs(ctx, tc.req)
			assert.Error(t, err)
			assert.Nil(t, resp)
			cancel()
		})
	}
}

func nEventSequenceEvents(n int) []*armadaevents.EventSequence_Event {
	events := make([]*armadaevents.EventSequence_Event, n)
	for i := 0; i < n; i++ {
		events[i] = &armadaevents.EventSequence_Event{
			Created: &baseTime,
			Event: &armadaevents.EventSequence_Event_SubmitJob{
				SubmitJob: &armadaevents.SubmitJob{
					JobId:           testUlid(i + 1),
					Priority:        defaultPriority,
					ObjectMeta:      &armadaevents.ObjectMeta{Namespace: defaultNamespace},
					Objects:         []*armadaevents.KubernetesObject{},
					DeduplicationId: fmt.Sprintf("%d", i+1),
					MainObject: &armadaevents.KubernetesMainObject{
						Object: &armadaevents.KubernetesMainObject_PodSpec{
							PodSpec: &armadaevents.PodSpecWithAvoidList{
								PodSpec: &v1.PodSpec{
									TerminationGracePeriodSeconds: pointer.Int64(defaultTerminationGracePeriodSeconds),
									ActiveDeadlineSeconds:         pointer.Int64(defaultActiveDeadlineSeconds),
									PriorityClassName:             defaultPriorityClass,
									Tolerations:                   defaultTolerations,
									Containers:                    defaultContainer,
								},
							},
						},
					},
				},
			},
		}
	}
	return events
}

func submitRequestWithNItems(n int) *api.JobSubmitRequest {
	items := make([]*api.JobSubmitRequestItem, n)
	for i := 0; i < n; i++ {
		items[i] = &api.JobSubmitRequestItem{
			Priority:  float64(defaultPriority),
			Namespace: defaultNamespace,
			ClientId:  fmt.Sprintf("%d", i+1),
			PodSpec: &v1.PodSpec{
				TerminationGracePeriodSeconds: pointer.Int64(defaultTerminationGracePeriodSeconds),
				ActiveDeadlineSeconds:         pointer.Int64(defaultActiveDeadlineSeconds),
				PriorityClassName:             defaultPriorityClass,
				Containers:                    defaultContainer,
			},
		}
	}
	return &api.JobSubmitRequest{
		Queue:           "testQueue",
		JobSetId:        "testJobset",
		JobRequestItems: items,
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
		for i, container := range item.PodSpec.Containers {
			container.Resources = r
			item.PodSpec.Containers[i] = container
		}
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

func defaultSubmissionConfig() configuration.SubmissionConfig {
	return configuration.SubmissionConfig{
		AllowedPriorityClassNames: map[string]bool{defaultPriorityClass: true},
		DefaultPriorityClassName:  defaultPriorityClass,
		DefaultJobLimits:          armadaresource.ComputeResources{"cpu": resource.MustParse("1")},
		DefaultJobTolerations:     defaultTolerations,
		MaxPodSpecSizeBytes:       1000,
		MinJobResources:           map[v1.ResourceName]resource.Quantity{},
		MinTerminationGracePeriod: 30 * time.Second,
		MaxTerminationGracePeriod: 300 * time.Second,
		DefaultActiveDeadline:     1 * time.Hour,
	}
}

func testUlid(i int) *armadaevents.Uuid {
	ulid := fmt.Sprintf("000000000000000000000000%02X", i)
	return armadaevents.MustProtoUuidFromUlidString(ulid)
}
