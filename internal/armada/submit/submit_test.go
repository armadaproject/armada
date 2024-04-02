package submit

import (
	"context"
	"fmt"
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

var defaultSubmissionConfig = configuration.SubmissionConfig{
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

var defaultQueue = queue.Queue{
	Name: "testQueue",
	Permissions: []queue.Permissions{
		{
			Subjects: []queue.PermissionSubject{{
				Kind: "Group",
				Name: "submit-job-group",
			}},
			Verbs: []queue.PermissionVerb{queue.PermissionVerbSubmit},
		},
	},
}

var defaultPrincipal = *authorization.NewStaticPrincipal("testUser", []string{"groupA"})

var baseTime = time.Now()

func TestSubmit(t *testing.T) {

	tests := map[string]struct {
		req                   *api.JobSubmitRequest
		submissionConfig      configuration.SubmissionConfig
		expectedEventSequence *armadaevents.EventSequence
		queue                 queue.Queue
		deduplicationIds      map[string]string
		principal             authorization.StaticPrincipal
		authorized            bool
		expectSuccess         bool
	}{
		"valid request": {
			submissionConfig: defaultSubmissionConfig,
			queue:            defaultQueue,
			deduplicationIds: map[string]string{},
			req:              minimalValidSubmission(),
			principal:        defaultPrincipal,
			expectedEventSequence: &armadaevents.EventSequence{
				Queue:      defaultQueue.Name,
				JobSetName: "testJobset",
				UserId:     "testUser",
				Groups:     []string{"groupA"},
				Events: []*armadaevents.EventSequence_Event{
					{
						Created: &baseTime,
						Event: &armadaevents.EventSequence_Event_SubmitJob{
							SubmitJob: &armadaevents.SubmitJob{
								JobId:      armadaevents.MustProtoUuidFromUlidString("00000000000000000000000001"),
								Priority:   1000,
								ObjectMeta: &armadaevents.ObjectMeta{},
								MainObject: &armadaevents.KubernetesMainObject{
									ObjectMeta: nil,
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
					},
				},
			},
			expectSuccess: true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
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
				tc.submissionConfig,
				deduplicator,
				submitChecker,
				authorizer)

			server.clock = clock.NewFakeClock(baseTime)
			server.idGenerator = testUlidGenerator

			var capturedEventMessage *armadaevents.EventSequence

			queueRepository.EXPECT().GetQueue(gomock.Any(), tc.req.Queue).Return(tc.queue, nil)
			authorizer.EXPECT().AuthorizeQueueAction(gomock.Any(), tc.queue, permissions.SubmitAnyJobs, queue.PermissionVerbSubmit).Return(nil).Times(1)
			deduplicator.EXPECT().GetOriginalJobIds(gomock.Any(), tc.queue.Name, tc.req.JobRequestItems).Return(tc.deduplicationIds, nil).Times(1)
			deduplicator.EXPECT().StoreOriginalJobIds(gomock.Any(), tc.queue.Name, gomock.Any())
			submitChecker.EXPECT().CheckApiJobs(gomock.Any()).Return(true, "").Times(1)
			publisher.EXPECT().
				PublishMessages(gomock.Any(), gomock.Any()).
				Times(1).
				Do(func(_ interface{}, es *armadaevents.EventSequence) {
					capturedEventMessage = es
				})

			resp, err := server.SubmitJobs(context.TODO(), tc.req)
			if tc.expectSuccess {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedEventSequence, capturedEventMessage)
				assert.Equal(t, len(tc.req.JobRequestItems), len(resp.JobResponseItems))
			} else {
				assert.Error(t, err)
			}
		})
	}
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
