package submit

import (
	"fmt"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"k8s.io/apimachinery/pkg/util/clock"
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
	defaultQueue     = queue.Queue{Name: "testQueue"}
	defaultPrincipal = authorization.NewStaticPrincipal("testUser", []string{"groupA"})
	baseTime         = time.Now().UTC()
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

func TestSuccessfulSubmit(t *testing.T) {

	tests := map[string]struct {
		req              *api.JobSubmitRequest
		deduplicationIds map[string]string
		expectedEvents   []*armadaevents.EventSequence_Event
	}{
		"single job request": {
			req:            minimalValidSubmission(),
			expectedEvents: []*armadaevents.EventSequence_Event{minimalEventSequenceEvent()},
		},
		"multiple job request": {
			req:            minimalValidSubmission(),
			expectedEvents: []*armadaevents.EventSequence_Event{minimalEventSequenceEvent()},
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
				Groups:     []string{"groupA", "everyone"},
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
				ulid := fmt.Sprintf("000000000000000000000000%02X", counter)
				return armadaevents.MustProtoUuidFromUlidString(ulid)
			}

			server := NewServer(
				publisher,
				queueRepository,
				jobRepository,
				defaultSubmissionConfig,
				deduplicator,
				submitChecker,
				authorizer)

			server.clock = clock.NewFakeClock(baseTime)
			server.idGenerator = testUlidGenerator

			queueRepository.EXPECT().GetQueue(ctx, tc.req.Queue).Return(defaultQueue, nil)
			authorizer.EXPECT().AuthorizeQueueAction(ctx, defaultQueue, permissions.SubmitAnyJobs, queue.PermissionVerbSubmit).Return(nil).Times(1)
			deduplicator.EXPECT().GetOriginalJobIds(ctx, defaultQueue.Name, tc.req.JobRequestItems).Return(tc.deduplicationIds, nil).Times(1)
			deduplicator.EXPECT().StoreOriginalJobIds(ctx, defaultQueue.Name, gomock.Any())
			submitChecker.EXPECT().CheckApiJobs(expectedEventSequence).Return(true, "").Times(1)
			jobRepository.EXPECT().StorePulsarSchedulerJobDetails(ctx, []*schedulerobjects.PulsarSchedulerJobDetails{
				{
					JobId:  "00000000000000000000000001",
					Queue:  "testQueue",
					JobSet: "testJobset",
				},
			}).Return(nil).Times(1)

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

func minimalEventSequenceEvent() *armadaevents.EventSequence_Event {
	return &armadaevents.EventSequence_Event{
		Created: &baseTime,
		Event: &armadaevents.EventSequence_Event_SubmitJob{
			SubmitJob: &armadaevents.SubmitJob{
				JobId:      armadaevents.MustProtoUuidFromUlidString("00000000000000000000000001"),
				Priority:   1000,
				ObjectMeta: &armadaevents.ObjectMeta{Namespace: "testNamespace"},
				Objects:    []*armadaevents.KubernetesObject{},
				MainObject: &armadaevents.KubernetesMainObject{
					Object: &armadaevents.KubernetesMainObject_PodSpec{
						PodSpec: &armadaevents.PodSpecWithAvoidList{
							PodSpec: &v1.PodSpec{
								Containers: []v1.Container{
									{
										Resources: v1.ResourceRequirements{
											Requests: v1.ResourceList{
												"cpu":    resource.MustParse("1"),
												"memory": resource.MustParse("1Gi"),
											},
											Limits: v1.ResourceList{
												"cpu":    resource.MustParse("1"),
												"memory": resource.MustParse("1Gi"),
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func MinimalValidSubmissions() *api.JobSubmitRequest {
	return nil
}

func minimalValidSubmission() *api.JobSubmitRequest {
	return &api.JobSubmitRequest{
		Queue:    "testQueue",
		JobSetId: "testJobset",
		JobRequestItems: []*api.JobSubmitRequestItem{
			{
				Priority:  1000,
				Namespace: "testNamespace",
				PodSpec: &v1.PodSpec{
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									"cpu":    resource.MustParse("1"),
									"memory": resource.MustParse("1Gi"),
								},
								Limits: v1.ResourceList{
									"cpu":    resource.MustParse("1"),
									"memory": resource.MustParse("1Gi"),
								},
							},
						},
					},
				},
			},
		},
	}
}

func defaultSubmissionConfig() configuration.SubmissionConfig {
	return configuration.SubmissionConfig{
		AllowedPriorityClassNames: map[string]bool{"pc1": true, "pc2": true},
		DefaultPriorityClassName:  "pc1",
		DefaultJobLimits:          armadaresource.ComputeResources{"cpu": resource.MustParse("1")},
		DefaultJobTolerations: []v1.Toleration{
			{
				Key:      "armadaproject.io/foo",
				Operator: "Exists",
			},
		},
		DefaultJobTolerationsByResourceRequest: map[string][]v1.Toleration{
			"nvidia.com/gpu": {
				{
					Key:      "armadaproject.io/gpuNode",
					Operator: "Exists",
				},
			},
		},
		MaxPodSpecSizeBytes:            1000,
		MinJobResources:                map[v1.ResourceName]resource.Quantity{},
		DefaultGangNodeUniformityLabel: "",
		MinTerminationGracePeriod:      30 * time.Second,
		MaxTerminationGracePeriod:      300 * time.Second,
		DefaultActiveDeadline:          1 * time.Hour,
		DefaultActiveDeadlineByResourceRequest: map[string]time.Duration{
			"nvidia.com/gpu": 24 * time.Hour,
		},
	}
}
