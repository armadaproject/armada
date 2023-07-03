package server

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"testing/quick"
	"time"

	"github.com/go-redis/redis"
	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/armada/permissions"
	"github.com/armadaproject/armada/internal/armada/repository"
	"github.com/armadaproject/armada/internal/common/auth/authorization"
	"github.com/armadaproject/armada/internal/common/auth/permission"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	schedulertypes "github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client/queue"
)

func TestSubmitServer_HealthCheck(t *testing.T) {
	withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
		health, err := s.Health(context.Background(), &types.Empty{})
		assert.NoError(t, err)
		assert.Equal(t, health.Status, api.HealthCheckResponse_SERVING)
	})
}

func TestSubmitServer_CreateQueue_WithDefaultSettings_CanBeReadBack(t *testing.T) {
	withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
		const queueName = "myQueue"
		const priority = 1.0

		_, err := s.CreateQueue(context.Background(), &api.Queue{Name: queueName, PriorityFactor: priority})
		assert.NoError(t, err)

		receivedQueue, err := s.GetQueue(context.Background(), &api.QueueGetRequest{Name: queueName})
		assert.NoError(t, err)

		defaultQueue := &api.Queue{Name: queueName, PriorityFactor: priority, UserOwners: []string{"anonymous"}, GroupOwners: nil, ResourceLimits: nil}

		q1, err := queue.NewQueue(receivedQueue)
		assert.NoError(t, err)

		q2, err := queue.NewQueue(defaultQueue)
		assert.NoError(t, err)

		assert.Equal(t, q1, q2)
	})
}

func TestSubmitServer_CreateQueue_WithCustomSettings_CanBeReadBack(t *testing.T) {
	withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
		const queueName = "myQueue"
		originalQueue := &api.Queue{
			Name:           queueName,
			PriorityFactor: 1.1,
			UserOwners:     []string{"user-a", "user-b"},
			GroupOwners:    []string{"group-a", "group-b"},
			ResourceLimits: map[string]float64{"memory": 0.2, "cpu": 0.3},
		}

		_, err := s.CreateQueue(context.Background(), originalQueue)
		assert.NoError(t, err)

		roundTrippedQueue, err := s.GetQueue(context.Background(), &api.QueueGetRequest{Name: queueName})
		assert.NoError(t, err)

		q1, err := queue.NewQueue(originalQueue)
		assert.NoError(t, err)

		q2, err := queue.NewQueue(roundTrippedQueue)
		assert.NoError(t, err)

		assert.Equal(t, q1, q2)
	})
}

func TestSubmitServer_CreateQueue_WhenQueueAlreadyExists_QueueIsNotChanged_AndReturnsAlreadyExists(t *testing.T) {
	withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
		const queueName = "myQueue"
		originalQueue := &api.Queue{
			Name:           queueName,
			PriorityFactor: 1.1,
			UserOwners:     []string{"user-a", "user-b"},
			GroupOwners:    []string{"group-a", "group-b"},
			ResourceLimits: map[string]float64{"cpu": 0.2, "memory": 0.3},
		}

		_, err := s.CreateQueue(context.Background(), originalQueue)
		assert.NoError(t, err)

		_, err = s.CreateQueue(
			context.Background(),
			&api.Queue{
				Name:           queueName,
				PriorityFactor: 2,
				UserOwners:     []string{"user-c"},
				GroupOwners:    []string{"group-c"},
				ResourceLimits: map[string]float64{"cpu": 0.4},
			},
		)
		assert.Equal(t, codes.AlreadyExists, status.Code(err))

		roundTrippedQueue, err := s.GetQueue(context.Background(), &api.QueueGetRequest{Name: queueName})
		assert.NoError(t, err)

		q1, err := queue.NewQueue(originalQueue)
		assert.NoError(t, err)

		q2, err := queue.NewQueue(roundTrippedQueue)
		assert.NoError(t, err)

		assert.Equal(t, q1, q2)
	})
}

func TestSubmitServer_UpdateQueue_WhenQueueDoesNotExist_DoesNotCreateQueue_AndReturnsNotFound(t *testing.T) {
	withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
		const queueName = "non_existent_queue"

		_, err := s.UpdateQueue(context.Background(), &api.Queue{Name: queueName, PriorityFactor: 1})
		assert.Equal(t, codes.NotFound, status.Code(err))

		_, err = s.GetQueue(context.Background(), &api.QueueGetRequest{Name: queueName})
		assert.Equal(t, codes.NotFound, status.Code(err))
	})
}

func TestSubmitServer_UpdateQueue_WhenQueueExists_ReplacesQueue(t *testing.T) {
	withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
		const queueName = "myQueue"

		originalQueue := &api.Queue{
			Name:           queueName,
			PriorityFactor: 1.1,
			UserOwners:     []string{"user-a", "user-b"},
			GroupOwners:    []string{"group-a", "group-b"},
			ResourceLimits: map[string]float64{"cpu": 0.2, "memory": 0.3},
		}
		_, err := s.CreateQueue(context.Background(), originalQueue)
		assert.NoError(t, err)

		updatedQueue := &api.Queue{
			Name:           queueName,
			PriorityFactor: 2.2,
			UserOwners:     []string{"user-a", "user-c"},
			GroupOwners:    []string{"group-c", "group-b"},
			ResourceLimits: map[string]float64{"cpu": 0.3, "memory": 0.3},
		}
		_, err = s.UpdateQueue(context.Background(), updatedQueue)
		assert.NoError(t, err)

		receivedQueue, err := s.GetQueue(context.Background(), &api.QueueGetRequest{Name: queueName})
		assert.NoError(t, err)

		q1, err := queue.NewQueue(updatedQueue)
		assert.NoError(t, err)

		q2, err := queue.NewQueue(receivedQueue)
		assert.NoError(t, err)

		assert.Equal(t, q1, q2)
	})
}

func TestSubmitServer_CreateQueue_WhenPermissionsCheckFails_QueueIsNotCreated_AndReturnsPermissionDenied(t *testing.T) {
	withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
		const queueName = "myQueue"

		s.permissions = &FakeDenyAllPermissionChecker{}

		_, err := s.CreateQueue(context.Background(), &api.Queue{Name: queueName, PriorityFactor: 1})
		assert.Equal(t, codes.PermissionDenied, status.Code(err))

		_, err = s.GetQueue(context.Background(), &api.QueueGetRequest{Name: queueName})
		assert.Equal(t, codes.NotFound, status.Code(err))
	})
}

func TestSubmitServer_UpdateQueue_WhenPermissionsCheckFails_QueueIsNotUpdated_AndReturnsPermissionDenied(t *testing.T) {
	withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
		const queueName = "myQueue"
		originalQueue := &api.Queue{Name: queueName, PriorityFactor: 1}

		_, err := s.CreateQueue(context.Background(), originalQueue)
		assert.NoError(t, err)

		s.permissions = &FakeDenyAllPermissionChecker{}

		_, err = s.UpdateQueue(context.Background(), &api.Queue{Name: queueName, PriorityFactor: originalQueue.PriorityFactor + 100})
		assert.Equal(t, codes.PermissionDenied, status.Code(err))

		receivedQueue, err := s.GetQueue(context.Background(), &api.QueueGetRequest{Name: queueName})
		assert.NoError(t, err)

		q1, err := queue.NewQueue(originalQueue)
		assert.NoError(t, err)

		q2, err := queue.NewQueue(receivedQueue)
		assert.NoError(t, err)

		assert.Equal(t, q1, q2)
	})
}

func TestSubmitServer_DeleteQueue_WhenPermissionsCheckFails_QueueIsNotDelete_AndReturnsPermissionDenied(t *testing.T) {
	withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
		const queueName = "myQueue"
		originalQueue := &api.Queue{Name: queueName, PriorityFactor: 1}

		_, err := s.CreateQueue(context.Background(), originalQueue)
		assert.NoError(t, err)

		s.permissions = &FakeDenyAllPermissionChecker{}

		_, err = s.DeleteQueue(context.Background(), &api.QueueDeleteRequest{Name: queueName})
		assert.Equal(t, codes.PermissionDenied, status.Code(err))

		receivedQueue, err := s.GetQueue(context.Background(), &api.QueueGetRequest{Name: queueName})
		assert.NoError(t, err)

		q1, err := queue.NewQueue(originalQueue)
		assert.NoError(t, err)
		q2, err := queue.NewQueue(receivedQueue)
		assert.NoError(t, err)

		assert.Equal(t, q1, q2)
	})
}

func TestSubmitServer_SubmitJob(t *testing.T) {
	withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
		jobSetId := util.NewULID()
		jobRequest := createJobRequest(jobSetId, 1)

		response, err := s.SubmitJobs(context.Background(), jobRequest)

		if ok := assert.NoError(t, err); !ok {
			t.FailNow()
		}
		if ok := assert.NotEmpty(t, response.JobResponseItems); !ok {
			t.FailNow()
		}
		assert.NotEmpty(t, response.JobResponseItems[0].JobId)
	})
}

func TestSubmitServer_SubmitJob_ApplyDefaults(t *testing.T) {
	withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
		jobSetId := util.NewULID()
		jobRequest := &api.JobSubmitRequest{
			JobSetId: jobSetId,
			Queue:    "test",
			JobRequestItems: []*api.JobSubmitRequestItem{
				{
					ClientId: util.NewULID(),
					PodSpecs: []*v1.PodSpec{{
						Containers: []v1.Container{
							{
								Name:  fmt.Sprintf("Container 1"),
								Image: "index.docker.io/library/ubuntu:latest",
								Args:  []string{"sleep", "10s"},
							},
						},
					}},
					Priority: 0,
				},
			},
		}
		response, err := s.SubmitJobs(context.Background(), jobRequest)
		assert.Empty(t, err)

		retrievedJob, _ := s.jobRepository.GetExistingJobsByIds([]string{response.JobResponseItems[0].JobId})

		expectedResources := v1.ResourceList{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")}
		expectedTolerations := []v1.Toleration{
			{
				Key:      "default",
				Operator: v1.TolerationOpEqual,
				Value:    "true",
				Effect:   v1.TaintEffectNoSchedule,
			},
		}
		expectedTerminationGracePeriodSeconds := int64(s.schedulingConfig.MinTerminationGracePeriod.Seconds())

		assert.Equal(t, expectedResources, retrievedJob[0].PodSpec.Containers[0].Resources.Requests)
		assert.Equal(t, expectedResources, retrievedJob[0].PodSpec.Containers[0].Resources.Limits)
		assert.Equal(t, expectedTolerations, retrievedJob[0].PodSpec.Tolerations)
		assert.Equal(t, expectedTerminationGracePeriodSeconds, *retrievedJob[0].PodSpec.TerminationGracePeriodSeconds)
	})
}

func TestSubmitServer_SubmitJob_RejectEmptyPodSpec(t *testing.T) {
	withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
		jobSetId := util.NewULID()
		jobRequest := &api.JobSubmitRequest{
			JobSetId: jobSetId,
			Queue:    "test",
			JobRequestItems: []*api.JobSubmitRequestItem{
				{
					ClientId: util.NewULID(),
					Priority: 0,
				},
			},
		}
		_, err := s.SubmitJobs(context.Background(), jobRequest)
		assert.NotEmpty(t, err)
	})
}

func TestSubmitServer_SubmitJob_RejectTolerationsNotEqual(t *testing.T) {
	withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
		jobSetId := util.NewULID()
		jobRequest := &api.JobSubmitRequest{
			JobSetId: jobSetId,
			Queue:    "test",
			JobRequestItems: []*api.JobSubmitRequestItem{
				{
					ClientId: util.NewULID(),
					PodSpecs: []*v1.PodSpec{{
						Containers: []v1.Container{
							{
								Name:  fmt.Sprintf("Container 1"),
								Image: "index.docker.io/library/ubuntu:latest",
								Args:  []string{"sleep", "10s"},
								Resources: v1.ResourceRequirements{
									Requests: v1.ResourceList{
										"memory": resource.MustParse("1Gi"),
										"cpu":    resource.MustParse("1"),
									},
									Limits: v1.ResourceList{
										"memory": resource.MustParse("1Gi"),
										"cpu":    resource.MustParse("2"),
									},
								},
							},
						},
					}},
					Priority: 0,
				},
			},
		}
		_, err := s.SubmitJobs(context.Background(), jobRequest)
		assert.NotEmpty(t, err)
	})
}

func TestSubmitServer_SubmitJob_RejectPodSpecAndPodSpecs(t *testing.T) {
	podSpec := v1.PodSpec{
		Containers: []v1.Container{
			{
				Name:  fmt.Sprintf("Container 1"),
				Image: "index.docker.io/library/ubuntu:latest",
				Args:  []string{"sleep", "10s"},
			},
		},
	}
	withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
		jobSetId := util.NewULID()
		jobRequest := &api.JobSubmitRequest{
			JobSetId: jobSetId,
			Queue:    "test",
			JobRequestItems: []*api.JobSubmitRequestItem{
				{
					ClientId: util.NewULID(),
					PodSpec:  &podSpec,
					PodSpecs: []*v1.PodSpec{&podSpec},
					Priority: 0,
				},
			},
		}
		_, err := s.SubmitJobs(context.Background(), jobRequest)
		assert.NotEmpty(t, err)
	})
}

func TestSubmitServer_SubmitJob_WhenPodCannotBeScheduled(t *testing.T) {
	withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
		jobSetId := util.NewULID()
		jobRequest := createJobRequest(jobSetId, 1)

		err := s.schedulingInfoRepository.UpdateClusterSchedulingInfo(&api.ClusterSchedulingInfoReport{
			ClusterId:  "test-cluster",
			ReportTime: time.Now(),
			NodeTypes: []*api.NodeType{{
				Taints:               nil,
				Labels:               nil,
				AllocatableResources: armadaresource.ComputeResources{"cpu": resource.MustParse("0"), "memory": resource.MustParse("0")},
			}},
		})
		assert.Empty(t, err)

		_, err = s.SubmitJobs(context.Background(), jobRequest)

		assert.Error(t, err)
	})
}

func TestSubmitServer_SubmitJob_AddsExpectedEventsInCorrectOrder(t *testing.T) {
	withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
		jobSetId := util.NewULID()
		jobRequest := createJobRequest(jobSetId, 1)

		_, err := s.SubmitJobs(context.Background(), jobRequest)
		assert.Empty(t, err)

		messages := events.ReceivedEvents
		assert.NoError(t, err)
		assert.Equal(t, len(messages), 2)

		firstEvent := messages[0]
		secondEvent := messages[1]

		// First event should be submitted
		assert.NotNil(t, firstEvent.GetSubmitted())
		// Second event should be queued
		assert.NotNil(t, secondEvent.GetQueued())
	})
}

func TestSubmitServer_SubmitJob_ReturnsJobItemsInTheSameOrderTheyWereSubmitted(t *testing.T) {
	withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
		jobSetId := util.NewULID()
		jobRequest := createJobRequest(jobSetId, 5)

		response, err := s.SubmitJobs(context.Background(), jobRequest)
		assert.Empty(t, err)

		jobIds := make([]string, 0, 5)

		for _, jobItem := range response.JobResponseItems {
			jobIds = append(jobIds, jobItem.JobId)
		}

		// Get jobs for jobIds returned
		jobs, _ := s.jobRepository.GetExistingJobsByIds(jobIds)
		jobSet := make(map[string]*api.Job, 5)
		for _, job := range jobs {
			jobSet[job.Id] = job
		}

		// Confirm submitted spec and created spec line up, using order of returned jobIds to correlate submitted to created
		for i := 0; i < len(jobRequest.JobRequestItems); i++ {
			requestItem := jobRequest.JobRequestItems[i]
			returnedId := jobIds[i]
			createdJob := jobSet[returnedId]

			assert.NotNil(t, createdJob)
			if requestItem.PodSpec != nil {
				assert.Equal(t, requestItem.PodSpec, createdJob.PodSpec)
			} else if len(requestItem.PodSpecs) == 1 {
				assert.Equal(t, requestItem.PodSpecs[0], createdJob.PodSpec)
			} else {
				assert.Equal(t, requestItem.PodSpecs, createdJob.PodSpecs)
			}
		}
	})
}

func TestSubmitServer_SubmitJobs_RejectsIfTooManyJobsAreQueued(t *testing.T) {
	withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
		limit := 3

		s.queueManagementConfig.DefaultQueuedJobsLimit = limit
		jobSetId := util.NewULID()

		for i := 0; i < limit; i++ {
			result, err := s.SubmitJobs(context.Background(), createJobRequest(jobSetId, 1))
			assert.NoError(t, err)
			assert.Len(t, result.JobResponseItems, 1)
		}

		_, err := s.SubmitJobs(context.Background(), createJobRequest(jobSetId, 1))
		assert.Error(t, err)
	})
}

func TestSubmitServer_ReprioritizeJobs(t *testing.T) {
	t.Run("job that doesn't exist", func(t *testing.T) {
		withSubmitServerAndRepos(func(s *SubmitServer, jobRepo repository.JobRepository, events *repository.TestEventStore) {
			reprioritizeResponse, err := s.ReprioritizeJobs(context.Background(), &api.JobReprioritizeRequest{
				JobIds:      []string{util.NewULID()},
				NewPriority: 123,
			})
			assert.NoError(t, err)
			assert.Equal(t, 0, len(reprioritizeResponse.ReprioritizationResults))
		})
	})

	t.Run("one job", func(t *testing.T) {
		withSubmitServerAndRepos(func(s *SubmitServer, jobRepo repository.JobRepository, events *repository.TestEventStore) {
			newPriority := 123.0

			jobSetId := util.NewULID()
			jobRequest := createJobRequest(jobSetId, 1)

			submitResult, err := s.SubmitJobs(context.Background(), jobRequest)
			assert.NoError(t, err)
			jobId := submitResult.JobResponseItems[0].JobId

			reprioritizeResponse, err := s.ReprioritizeJobs(context.Background(), &api.JobReprioritizeRequest{
				JobIds:      []string{jobId},
				NewPriority: newPriority,
			})
			assert.NoError(t, err)
			assert.Equal(t, 1, len(reprioritizeResponse.ReprioritizationResults))
			errorString, ok := reprioritizeResponse.ReprioritizationResults[jobId]
			assert.True(t, ok)
			assert.Empty(t, errorString)

			jobs, err := jobRepo.PeekQueue("test", 100)
			assert.NoError(t, err)
			assert.Equal(t, jobId, jobs[0].Id)
			assert.Equal(t, newPriority, jobs[0].Priority)

			messages := events.ReceivedEvents

			assert.Equal(t, 5, len(messages))

			assert.NotNil(t, messages[0].GetSubmitted())
			assert.NotNil(t, messages[1].GetQueued())
			assert.NotNil(t, messages[2].GetReprioritizing())
			assert.NotNil(t, messages[3].GetUpdated())
			assert.NotNil(t, messages[4].GetReprioritized())

			assert.Equal(t, newPriority, messages[3].GetUpdated().Job.Priority)
			assert.Equal(t, newPriority, messages[4].GetReprioritized().NewPriority)
		})
	})

	t.Run("multiple jobs", func(t *testing.T) {
		withSubmitServerAndRepos(func(s *SubmitServer, jobRepo repository.JobRepository, events *repository.TestEventStore) {
			jobSetId := util.NewULID()
			jobRequest := createJobRequest(jobSetId, 3)

			submitResult, err := s.SubmitJobs(context.Background(), jobRequest)
			assert.NoError(t, err)
			var jobIds []string
			for _, item := range submitResult.JobResponseItems {
				jobIds = append(jobIds, item.JobId)
			}

			reprioritizeResponse, err := s.ReprioritizeJobs(context.Background(), &api.JobReprioritizeRequest{
				JobIds:      jobIds,
				NewPriority: 256,
			})
			assert.NoError(t, err)
			assert.Equal(t, 3, len(reprioritizeResponse.ReprioritizationResults))

			jobs, err := jobRepo.GetExistingJobsByIds(jobIds)
			assert.NoError(t, err)
			for _, job := range jobs {
				assert.Equal(t, float64(256), job.Priority)
			}

			messages := events.ReceivedEvents
			assert.NoError(t, err)
			assert.Equal(t, 5*3, len(messages))
		})
	})

	t.Run("leased job", func(t *testing.T) {
		withSubmitServerAndRepos(func(s *SubmitServer, jobRepo repository.JobRepository, events *repository.TestEventStore) {
			jobSetId := util.NewULID()
			jobRequest := createJobRequest(jobSetId, 1)

			submitResult, err := s.SubmitJobs(context.Background(), jobRequest)
			assert.NoError(t, err)
			jobId := submitResult.JobResponseItems[0].JobId

			jobs, err := s.jobRepository.GetExistingJobsByIds([]string{jobId})
			assert.NoError(t, err)
			jobIdsByQueue := make(map[string][]string)
			for _, job := range jobs {
				jobIdsByQueue["test"] = append(jobIdsByQueue["test"], job.Id)
			}
			leased, err := jobRepo.TryLeaseJobs("some-cluster", jobIdsByQueue)
			assert.NoError(t, err)
			assert.Equal(t, 1, len(leased))
			assert.Equal(t, 1, len(leased["test"]))

			reprioritizeResponse, err := s.ReprioritizeJobs(context.Background(), &api.JobReprioritizeRequest{
				JobIds:      []string{jobId},
				NewPriority: 123,
			})
			assert.NoError(t, err)
			assert.Equal(t, 1, len(reprioritizeResponse.ReprioritizationResults))
			errorString, ok := reprioritizeResponse.ReprioritizationResults[jobId]
			assert.True(t, ok)
			assert.Empty(t, errorString)

			jobs, err = jobRepo.GetExistingJobsByIds([]string{jobId})
			assert.NoError(t, err)
			assert.Equal(t, float64(123), jobs[0].Priority)

			messages := events.ReceivedEvents
			assert.NoError(t, err)
			assert.Equal(t, 5, len(messages))

			assert.NotNil(t, messages[0].GetSubmitted())
			assert.NotNil(t, messages[1].GetQueued())
			assert.NotNil(t, messages[2].GetReprioritizing())
			assert.NotNil(t, messages[3].GetUpdated())
			assert.NotNil(t, messages[4].GetReprioritized())
		})
	})

	t.Run("all jobs in a job set", func(t *testing.T) {
		withSubmitServerAndRepos(func(s *SubmitServer, jobRepo repository.JobRepository, events *repository.TestEventStore) {
			jobSetId := util.NewULID()
			jobRequest := createJobRequest(jobSetId, 3)

			submitResult, err := s.SubmitJobs(context.Background(), jobRequest)
			assert.NoError(t, err)
			var jobIds []string
			for _, item := range submitResult.JobResponseItems {
				jobIds = append(jobIds, item.JobId)
			}

			reprioritizeResponse, err := s.ReprioritizeJobs(context.Background(), &api.JobReprioritizeRequest{
				JobSetId:    jobSetId,
				Queue:       "test",
				NewPriority: 678,
			})
			assert.NoError(t, err)
			assert.Equal(t, 3, len(reprioritizeResponse.ReprioritizationResults))

			jobs, err := jobRepo.GetExistingJobsByIds(jobIds)
			assert.NoError(t, err)
			for _, job := range jobs {
				assert.Equal(t, float64(678), job.Priority)
			}

			messages := events.ReceivedEvents
			assert.NoError(t, err)
			assert.Equal(t, 5*3, len(messages))
		})
	})

	t.Run("updating priority after lease keeps priority", func(t *testing.T) {
		withSubmitServerAndRepos(func(s *SubmitServer, jobRepo repository.JobRepository, events *repository.TestEventStore) {
			jobSetId := util.NewULID()
			jobRequest := createJobRequest(jobSetId, 3)

			submitResult, err := s.SubmitJobs(context.Background(), jobRequest)
			assert.NoError(t, err)
			var jobIds []string
			for _, item := range submitResult.JobResponseItems {
				jobIds = append(jobIds, item.JobId)
			}

			jobs, err := s.jobRepository.GetExistingJobsByIds(jobIds)
			assert.NoError(t, err)
			selectedJob := jobs[1]
			clusterId := "some-cluster"

			leased, err := jobRepo.TryLeaseJobs(clusterId, map[string][]string{"test": {selectedJob.Id}})
			assert.NoError(t, err)
			assert.Equal(t, 1, len(leased))

			reprioritizeResponse, err := s.ReprioritizeJobs(context.Background(), &api.JobReprioritizeRequest{
				JobIds:      []string{selectedJob.Id},
				NewPriority: 1000,
			})
			assert.NoError(t, err)
			assert.Equal(t, 1, len(reprioritizeResponse.ReprioritizationResults))
			errorString, ok := reprioritizeResponse.ReprioritizationResults[selectedJob.Id]
			assert.True(t, ok)
			assert.Empty(t, errorString)

			_, err = jobRepo.ReturnLease(clusterId, selectedJob.Id)
			assert.NoError(t, err)

			jobs, err = jobRepo.PeekQueue("test", 100)
			assert.NoError(t, err)
			assert.Equal(t, selectedJob.Id, jobs[2].Id)
			assert.Equal(t, float64(1000), jobs[2].Priority)
		})
	})
}

func TestFillContainerRequestAndLimits(t *testing.T) {
	testCases := map[string]interface{}{
		"limitsNotSet": func(containers []v1.Container) bool {
			for index := range containers {
				containers[index].Resources.Limits = nil
			}
			fillContainerRequestsAndLimits(containers)

			for _, container := range containers {
				resources := container.Resources
				if !reflect.DeepEqual(resources.Requests, resources.Limits) {
					return false
				}
			}

			return true
		},
		"requestsNotSet": func(containers []v1.Container) bool {
			for index := range containers {
				containers[index].Resources.Requests = nil
			}
			fillContainerRequestsAndLimits(containers)

			for _, container := range containers {
				resources := container.Resources
				if !reflect.DeepEqual(resources.Requests, resources.Limits) {
					return false
				}
			}

			return true
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(subT *testing.T) {
			if err := quick.Check(testCase, nil); err != nil {
				subT.Fatal(err)
			}
		})
	}
}

func TestSubmitServer_GetQueueInfo_Permissions(t *testing.T) {
	const watchEventsGroup = "watch-events-group"
	const watchAllEventsGroup = "watch-all-events-group"
	const watchQueueGroup = "watch-queue-group"

	emptyPerms := make(map[permission.Permission][]string)
	perms := map[permission.Permission][]string{
		permissions.WatchEvents:    {watchEventsGroup},
		permissions.WatchAllEvents: {watchAllEventsGroup},
	}
	q := queue.Queue{
		Name: "test-queue",
		Permissions: []queue.Permissions{
			{
				Subjects: []queue.PermissionSubject{{
					Kind: queue.PermissionSubjectKindGroup,
					Name: watchQueueGroup,
				}},
				Verbs: []queue.PermissionVerb{queue.PermissionVerbWatch},
			},
		},
		PriorityFactor: 1,
	}

	t.Run("no permissions", func(t *testing.T) {
		withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
			s.permissions = authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms)
			err := s.queueRepository.CreateQueue(q)
			assert.NoError(t, err)

			principal := authorization.NewStaticPrincipal("alice", []string{})
			ctx := authorization.WithPrincipal(context.Background(), principal)

			_, err = s.GetQueueInfo(ctx, &api.QueueInfoRequest{
				Name: "test-queue",
			})
			e, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.PermissionDenied, e.Code())
		})
	})

	t.Run("global permission", func(t *testing.T) {
		withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
			s.permissions = authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms)
			err := s.queueRepository.CreateQueue(q)
			assert.NoError(t, err)

			principal := authorization.NewStaticPrincipal("alice", []string{watchAllEventsGroup})
			ctx := authorization.WithPrincipal(context.Background(), principal)

			_, err = s.GetQueueInfo(ctx, &api.QueueInfoRequest{
				Name: "test-queue",
			})
			e, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.OK, e.Code())
		})
	})

	t.Run("queue permission without specific global permission", func(t *testing.T) {
		withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
			s.permissions = authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms)
			err := s.queueRepository.CreateQueue(q)
			assert.NoError(t, err)

			principal := authorization.NewStaticPrincipal("alice", []string{watchQueueGroup})
			ctx := authorization.WithPrincipal(context.Background(), principal)

			_, err = s.GetQueueInfo(ctx, &api.QueueInfoRequest{
				Name: "test-queue",
			})
			e, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.PermissionDenied, e.Code())
		})
	})

	t.Run("queue permission", func(t *testing.T) {
		withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
			s.permissions = authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms)
			err := s.queueRepository.CreateQueue(q)
			assert.NoError(t, err)

			principal := authorization.NewStaticPrincipal("alice", []string{watchEventsGroup, watchQueueGroup})
			ctx := authorization.WithPrincipal(context.Background(), principal)

			_, err = s.GetQueueInfo(ctx, &api.QueueInfoRequest{
				Name: "test-queue",
			})
			e, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.OK, e.Code())
		})
	})
}

func TestSubmitServer_CreateQueue_Permissions(t *testing.T) {
	emptyPerms := make(map[permission.Permission][]string)
	perms := map[permission.Permission][]string{
		permissions.CreateQueue: {"create-queue-group"},
	}

	t.Run("no permissions", func(t *testing.T) {
		withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
			s.permissions = authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms)

			principal := authorization.NewStaticPrincipal("alice", []string{})
			ctx := authorization.WithPrincipal(context.Background(), principal)

			_, err := s.CreateQueue(ctx, &api.Queue{
				Name:           "test-queue",
				PriorityFactor: 1,
			})
			e, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.PermissionDenied, e.Code())
		})
	})

	t.Run("global permissions", func(t *testing.T) {
		withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
			s.permissions = authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms)

			principal := authorization.NewStaticPrincipal("alice", []string{"create-queue-group"})
			ctx := authorization.WithPrincipal(context.Background(), principal)

			_, err := s.CreateQueue(ctx, &api.Queue{
				Name:           "test-queue",
				PriorityFactor: 1,
			})
			e, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.OK, e.Code())
		})
	})
}

func TestSubmitServer_UpdateQueue_Permissions(t *testing.T) {
	emptyPerms := make(map[permission.Permission][]string)
	perms := map[permission.Permission][]string{
		permissions.CreateQueue: {"create-queue-group"},
	}
	q := queue.Queue{
		Name:           "test-queue",
		PriorityFactor: 1,
	}

	t.Run("no permissions", func(t *testing.T) {
		withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
			s.permissions = authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms)
			err := s.queueRepository.CreateQueue(q)
			assert.NoError(t, err)

			principal := authorization.NewStaticPrincipal("alice", []string{})
			ctx := authorization.WithPrincipal(context.Background(), principal)

			_, err = s.UpdateQueue(ctx, &api.Queue{
				Name:           "test-queue",
				PriorityFactor: 1,
			})
			e, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.PermissionDenied, e.Code())
		})
	})

	t.Run("global permissions", func(t *testing.T) {
		withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
			s.permissions = authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms)
			err := s.queueRepository.CreateQueue(q)
			assert.NoError(t, err)

			principal := authorization.NewStaticPrincipal("alice", []string{"create-queue-group"})
			ctx := authorization.WithPrincipal(context.Background(), principal)

			_, err = s.UpdateQueue(ctx, &api.Queue{
				Name:           "test-queue",
				PriorityFactor: 1,
			})
			e, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.OK, e.Code())
		})
	})
}

func TestSubmitServer_DeleteQueue_Permissions(t *testing.T) {
	emptyPerms := make(map[permission.Permission][]string)
	perms := map[permission.Permission][]string{
		permissions.DeleteQueue: {"delete-queue-group"},
	}
	q := queue.Queue{
		Name:           "test-queue",
		PriorityFactor: 1,
	}

	t.Run("no permissions", func(t *testing.T) {
		withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
			s.permissions = authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms)
			err := s.queueRepository.CreateQueue(q)
			assert.NoError(t, err)

			principal := authorization.NewStaticPrincipal("alice", []string{})
			ctx := authorization.WithPrincipal(context.Background(), principal)

			_, err = s.DeleteQueue(ctx, &api.QueueDeleteRequest{Name: "test-queue"})
			e, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.PermissionDenied, e.Code())
		})
	})

	t.Run("global permissions", func(t *testing.T) {
		withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
			s.permissions = authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms)
			err := s.queueRepository.CreateQueue(q)
			assert.NoError(t, err)

			principal := authorization.NewStaticPrincipal("alice", []string{"delete-queue-group"})
			ctx := authorization.WithPrincipal(context.Background(), principal)

			_, err = s.DeleteQueue(ctx, &api.QueueDeleteRequest{Name: "test-queue"})
			e, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.OK, e.Code())
		})
	})
}

func TestSubmitServer_SubmitJobs_Permissions(t *testing.T) {
	emptyPerms := make(map[permission.Permission][]string)
	const testQueue = "test-queue"
	const testJobSet = "jobs-set-1"
	const submitJobsGroup = "submit-jobs-group"
	const submitAnyJobsGroup = "submit-any-jobs-group"
	const submitQueueGroup = "submit-queue-group"

	perms := map[permission.Permission][]string{
		permissions.SubmitJobs:    {submitJobsGroup},
		permissions.SubmitAnyJobs: {submitAnyJobsGroup},
	}
	q := queue.Queue{
		Name: testQueue,
		Permissions: []queue.Permissions{
			{
				Subjects: []queue.PermissionSubject{{
					Kind: queue.PermissionSubjectKindGroup,
					Name: submitQueueGroup,
				}},
				Verbs: []queue.PermissionVerb{queue.PermissionVerbSubmit},
			},
		},
		PriorityFactor: 1,
	}

	t.Run("no permissions: can't submit", func(t *testing.T) {
		withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
			s.permissions = authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms)
			err := s.queueRepository.CreateQueue(q)
			assert.NoError(t, err)

			principal := authorization.NewStaticPrincipal("alice", []string{})
			ctx := authorization.WithPrincipal(context.Background(), principal)

			_, err = s.SubmitJobs(ctx, &api.JobSubmitRequest{
				Queue:           testQueue,
				JobSetId:        testJobSet,
				JobRequestItems: createJobRequestItems(1),
			})
			e, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.PermissionDenied, e.Code())
		})
	})

	t.Run("lacks queue submit, but has global submit-any: can submit", func(t *testing.T) {
		withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
			s.permissions = authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms)
			err := s.queueRepository.CreateQueue(q)
			assert.NoError(t, err)

			principal := authorization.NewStaticPrincipal("alice", []string{submitAnyJobsGroup})
			ctx := authorization.WithPrincipal(context.Background(), principal)

			_, err = s.SubmitJobs(ctx, &api.JobSubmitRequest{
				Queue:           testQueue,
				JobSetId:        testJobSet,
				JobRequestItems: createJobRequestItems(1),
			})
			e, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.OK, e.Code())
		})
	})

	t.Run("has global submit, but lacks queue submit: can't submit", func(t *testing.T) {
		withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
			s.permissions = authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms)
			err := s.queueRepository.CreateQueue(q)
			assert.NoError(t, err)

			principal := authorization.NewStaticPrincipal("alice", []string{submitJobsGroup})
			ctx := authorization.WithPrincipal(context.Background(), principal)

			_, err = s.SubmitJobs(ctx, &api.JobSubmitRequest{
				Queue:           testQueue,
				JobSetId:        testJobSet,
				JobRequestItems: createJobRequestItems(1),
			})
			e, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.PermissionDenied, e.Code())
		})
	})

	t.Run("has queue submit, but lacks global submit or submit-any: can't submit", func(t *testing.T) {
		withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
			s.permissions = authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms)
			err := s.queueRepository.CreateQueue(q)
			assert.NoError(t, err)

			principal := authorization.NewStaticPrincipal("alice", []string{submitQueueGroup})
			ctx := authorization.WithPrincipal(context.Background(), principal)

			_, err = s.SubmitJobs(ctx, &api.JobSubmitRequest{
				Queue:           testQueue,
				JobSetId:        testJobSet,
				JobRequestItems: createJobRequestItems(1),
			})
			e, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.PermissionDenied, e.Code())
		})
	})

	t.Run("has queue submit & global submit-any: can submit", func(t *testing.T) {
		withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
			s.permissions = authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms)
			err := s.queueRepository.CreateQueue(q)
			assert.NoError(t, err)

			principal := authorization.NewStaticPrincipal("alice", []string{submitAnyJobsGroup, submitQueueGroup})
			ctx := authorization.WithPrincipal(context.Background(), principal)

			_, err = s.SubmitJobs(ctx, &api.JobSubmitRequest{
				Queue:           testQueue,
				JobSetId:        testJobSet,
				JobRequestItems: createJobRequestItems(1),
			})
			e, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.OK, e.Code())
		})
	})

	t.Run("has queue submit & global submit: can submit", func(t *testing.T) {
		withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
			s.permissions = authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms)
			err := s.queueRepository.CreateQueue(q)
			assert.NoError(t, err)

			principal := authorization.NewStaticPrincipal("alice", []string{submitJobsGroup, submitQueueGroup})
			ctx := authorization.WithPrincipal(context.Background(), principal)

			_, err = s.SubmitJobs(ctx, &api.JobSubmitRequest{
				Queue:           testQueue,
				JobSetId:        testJobSet,
				JobRequestItems: createJobRequestItems(1),
			})
			e, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.OK, e.Code())
		})
	})

	t.Run("no existing queue, no perms: can't create", func(t *testing.T) {
		withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
			s.queueManagementConfig.AutoCreateQueues = true
			s.permissions = authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms)

			principal := authorization.NewStaticPrincipal("alice", []string{})
			ctx := authorization.WithPrincipal(context.Background(), principal)

			_, err := s.SubmitJobs(ctx, &api.JobSubmitRequest{
				Queue:           testQueue,
				JobSetId:        testJobSet,
				JobRequestItems: createJobRequestItems(1),
			})
			e, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.PermissionDenied, e.Code())
		})
	})

	t.Run("no existing queue, has global submit: can't create", func(t *testing.T) {
		withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
			s.queueManagementConfig.AutoCreateQueues = true
			s.permissions = authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms)

			principal := authorization.NewStaticPrincipal("alice", []string{submitJobsGroup})
			ctx := authorization.WithPrincipal(context.Background(), principal)

			_, err := s.SubmitJobs(ctx, &api.JobSubmitRequest{
				Queue:           testQueue,
				JobSetId:        testJobSet,
				JobRequestItems: createJobRequestItems(1),
			})
			e, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.PermissionDenied, e.Code())
		})
	})

	t.Run("no existing queue, has global submit-any: can create", func(t *testing.T) {
		withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
			s.queueManagementConfig.AutoCreateQueues = true
			s.permissions = authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms)

			principal := authorization.NewStaticPrincipal("alice", []string{submitAnyJobsGroup})
			ctx := authorization.WithPrincipal(context.Background(), principal)

			_, err := s.SubmitJobs(ctx, &api.JobSubmitRequest{
				Queue:           testQueue,
				JobSetId:        testJobSet,
				JobRequestItems: createJobRequestItems(1),
			})
			e, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.OK, e.Code())
		})
	})

	t.Run("alice autocreates queue, rando bob cant submit to it", func(t *testing.T) {
		withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
			s.queueManagementConfig.AutoCreateQueues = true
			s.permissions = authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms)

			alice := authorization.NewStaticPrincipal("alice", []string{submitAnyJobsGroup})
			bob := authorization.NewStaticPrincipal("bob", []string{submitJobsGroup})
			alice_ctx := authorization.WithPrincipal(context.Background(), alice)
			bob_ctx := authorization.WithPrincipal(context.Background(), bob)

			_, err := s.SubmitJobs(alice_ctx, &api.JobSubmitRequest{
				Queue:           testQueue,
				JobSetId:        testJobSet,
				JobRequestItems: createJobRequestItems(1),
			})
			e, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.OK, e.Code())

			_, err = s.SubmitJobs(bob_ctx, &api.JobSubmitRequest{
				Queue:           testQueue,
				JobSetId:        testJobSet,
				JobRequestItems: createJobRequestItems(1),
			})
			e, ok = status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.PermissionDenied, e.Code())
		})
	})
}

func TestSubmitServer_CancelJobs_Permissions(t *testing.T) {
	emptyPerms := make(map[permission.Permission][]string)
	perms := map[permission.Permission][]string{
		permissions.CancelJobs:    {"cancel-jobs-group"},
		permissions.CancelAnyJobs: {"cancel-any-jobs-group"},
	}
	q := queue.Queue{
		Name: "test-queue",
		Permissions: []queue.Permissions{
			{
				Subjects: []queue.PermissionSubject{{
					Kind: "Group",
					Name: "cancel-queue-group",
				}},
				Verbs: []queue.PermissionVerb{queue.PermissionVerbCancel},
			},
		},
		PriorityFactor: 1,
	}
	job := &api.Job{
		Id:        util.NewULID(),
		JobSetId:  "job-set-1",
		Queue:     "test-queue",
		Namespace: "test-queue",
		Created:   time.Now(),
	}

	t.Run("no permissions", func(t *testing.T) {
		withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
			s.permissions = authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms)
			err := s.queueRepository.CreateQueue(q)
			assert.NoError(t, err)
			_, err = s.jobRepository.AddJobs([]*api.Job{job})
			assert.NoError(t, err)

			principal := authorization.NewStaticPrincipal("alice", []string{})
			ctx := authorization.WithPrincipal(context.Background(), principal)

			_, err = s.CancelJobs(ctx, &api.JobCancelRequest{
				Queue:    "test-queue",
				JobSetId: "job-set-1",
			})
			e, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.PermissionDenied, e.Code())
		})
	})

	t.Run("global permissions", func(t *testing.T) {
		withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
			s.permissions = authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms)
			err := s.queueRepository.CreateQueue(q)
			assert.NoError(t, err)
			_, err = s.jobRepository.AddJobs([]*api.Job{job})
			assert.NoError(t, err)

			principal := authorization.NewStaticPrincipal("alice", []string{"cancel-any-jobs-group"})
			ctx := authorization.WithPrincipal(context.Background(), principal)

			_, err = s.CancelJobs(ctx, &api.JobCancelRequest{
				Queue:    "test-queue",
				JobSetId: "job-set-1",
			})
			e, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.OK, e.Code())
		})
	})

	t.Run("queue permission without specific global permission", func(t *testing.T) {
		withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
			s.permissions = authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms)
			err := s.queueRepository.CreateQueue(q)
			assert.NoError(t, err)
			_, err = s.jobRepository.AddJobs([]*api.Job{job})
			assert.NoError(t, err)

			principal := authorization.NewStaticPrincipal("alice", []string{"cancel-queue-group"})
			ctx := authorization.WithPrincipal(context.Background(), principal)

			_, err = s.CancelJobs(ctx, &api.JobCancelRequest{
				Queue:    "test-queue",
				JobSetId: "job-set-1",
			})
			e, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.PermissionDenied, e.Code())
		})
	})

	t.Run("queue permission", func(t *testing.T) {
		withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
			s.permissions = authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms)
			err := s.queueRepository.CreateQueue(q)
			assert.NoError(t, err)
			_, err = s.jobRepository.AddJobs([]*api.Job{job})
			assert.NoError(t, err)

			principal := authorization.NewStaticPrincipal("alice", []string{"cancel-jobs-group", "cancel-queue-group"})
			ctx := authorization.WithPrincipal(context.Background(), principal)

			_, err = s.CancelJobs(ctx, &api.JobCancelRequest{
				Queue:    "test-queue",
				JobSetId: "job-set-1",
			})
			e, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.OK, e.Code())
		})
	})
}

func TestSubmitServer_CancelJobSet_Permissions(t *testing.T) {
	emptyPerms := make(map[permission.Permission][]string)
	perms := map[permission.Permission][]string{
		permissions.CancelJobs:    {"cancel-jobs-group"},
		permissions.CancelAnyJobs: {"cancel-any-jobs-group"},
	}
	q := queue.Queue{
		Name: "test-queue",
		Permissions: []queue.Permissions{
			{
				Subjects: []queue.PermissionSubject{{
					Kind: "Group",
					Name: "cancel-queue-group",
				}},
				Verbs: []queue.PermissionVerb{queue.PermissionVerbCancel},
			},
		},
		PriorityFactor: 1,
	}
	job := &api.Job{
		Id:        util.NewULID(),
		JobSetId:  "job-set-1",
		Queue:     "test-queue",
		Namespace: "test-queue",
		Created:   time.Now(),
	}

	t.Run("no permissions", func(t *testing.T) {
		withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
			s.permissions = authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms)
			err := s.queueRepository.CreateQueue(q)
			assert.NoError(t, err)
			_, err = s.jobRepository.AddJobs([]*api.Job{job})
			assert.NoError(t, err)

			principal := authorization.NewStaticPrincipal("alice", []string{})
			ctx := authorization.WithPrincipal(context.Background(), principal)

			_, err = s.CancelJobSet(ctx, &api.JobSetCancelRequest{
				Queue:    "test-queue",
				JobSetId: "job-set-1",
			})
			e, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.PermissionDenied, e.Code())
		})
	})

	t.Run("global permissions", func(t *testing.T) {
		withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
			s.permissions = authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms)
			err := s.queueRepository.CreateQueue(q)
			assert.NoError(t, err)
			_, err = s.jobRepository.AddJobs([]*api.Job{job})
			assert.NoError(t, err)

			principal := authorization.NewStaticPrincipal("alice", []string{"cancel-any-jobs-group"})
			ctx := authorization.WithPrincipal(context.Background(), principal)

			_, err = s.CancelJobSet(ctx, &api.JobSetCancelRequest{
				Queue:    "test-queue",
				JobSetId: "job-set-1",
			})
			e, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.OK, e.Code())
		})
	})

	t.Run("queue permission without specific global permission", func(t *testing.T) {
		withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
			s.permissions = authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms)
			err := s.queueRepository.CreateQueue(q)
			assert.NoError(t, err)
			_, err = s.jobRepository.AddJobs([]*api.Job{job})
			assert.NoError(t, err)

			principal := authorization.NewStaticPrincipal("alice", []string{"cancel-queue-group"})
			ctx := authorization.WithPrincipal(context.Background(), principal)

			_, err = s.CancelJobSet(ctx, &api.JobSetCancelRequest{
				Queue:    "test-queue",
				JobSetId: "job-set-1",
			})
			e, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.PermissionDenied, e.Code())
		})
	})

	t.Run("queue permission", func(t *testing.T) {
		withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
			s.permissions = authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms)
			err := s.queueRepository.CreateQueue(q)
			assert.NoError(t, err)
			_, err = s.jobRepository.AddJobs([]*api.Job{job})
			assert.NoError(t, err)

			principal := authorization.NewStaticPrincipal("alice", []string{"cancel-jobs-group", "cancel-queue-group"})
			ctx := authorization.WithPrincipal(context.Background(), principal)

			_, err = s.CancelJobSet(ctx, &api.JobSetCancelRequest{
				Queue:    "test-queue",
				JobSetId: "job-set-1",
			})
			e, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.OK, e.Code())
		})
	})
}

func TestSubmitServer_ReprioritizeJobs_Permissions(t *testing.T) {
	emptyPerms := make(map[permission.Permission][]string)
	perms := map[permission.Permission][]string{
		permissions.ReprioritizeJobs:    {"reprioritize-jobs-group"},
		permissions.ReprioritizeAnyJobs: {"reprioritize-any-jobs-group"},
	}
	q := queue.Queue{
		Name: "test-queue",
		Permissions: []queue.Permissions{
			{
				Subjects: []queue.PermissionSubject{{
					Kind: queue.PermissionSubjectKindGroup,
					Name: "reprioritize-queue-group",
				}},
				Verbs: []queue.PermissionVerb{queue.PermissionVerbReprioritize},
			},
		},
		PriorityFactor: 1,
	}
	job := &api.Job{
		Id:        util.NewULID(),
		JobSetId:  "job-set-1",
		Queue:     "test-queue",
		Namespace: "test-queue",
		Created:   time.Now(),
	}

	t.Run("no permissions", func(t *testing.T) {
		withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
			s.permissions = authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms)
			err := s.queueRepository.CreateQueue(q)
			assert.NoError(t, err)
			_, err = s.jobRepository.AddJobs([]*api.Job{job})
			assert.NoError(t, err)

			principal := authorization.NewStaticPrincipal("alice", []string{})
			ctx := authorization.WithPrincipal(context.Background(), principal)

			_, err = s.ReprioritizeJobs(ctx, &api.JobReprioritizeRequest{
				Queue:    "test-queue",
				JobSetId: "job-set-1",
			})
			e, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.PermissionDenied, e.Code())
		})
	})

	t.Run("global permissions", func(t *testing.T) {
		withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
			s.permissions = authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms)
			err := s.queueRepository.CreateQueue(q)
			assert.NoError(t, err)
			_, err = s.jobRepository.AddJobs([]*api.Job{job})
			assert.NoError(t, err)

			principal := authorization.NewStaticPrincipal("alice", []string{"reprioritize-any-jobs-group"})
			ctx := authorization.WithPrincipal(context.Background(), principal)

			_, err = s.ReprioritizeJobs(ctx, &api.JobReprioritizeRequest{
				Queue:    "test-queue",
				JobSetId: "job-set-1",
			})
			e, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.OK, e.Code())
		})
	})

	t.Run("queue permission without specific global permission", func(t *testing.T) {
		withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
			s.permissions = authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms)
			err := s.queueRepository.CreateQueue(q)
			assert.NoError(t, err)
			_, err = s.jobRepository.AddJobs([]*api.Job{job})
			assert.NoError(t, err)

			principal := authorization.NewStaticPrincipal("alice", []string{"reprioritize-queue-group"})
			ctx := authorization.WithPrincipal(context.Background(), principal)

			_, err = s.ReprioritizeJobs(ctx, &api.JobReprioritizeRequest{
				Queue:    "test-queue",
				JobSetId: "job-set-1",
			})
			e, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.PermissionDenied, e.Code())
		})
	})

	t.Run("queue permission", func(t *testing.T) {
		withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
			s.permissions = authorization.NewPrincipalPermissionChecker(perms, emptyPerms, emptyPerms)
			err := s.queueRepository.CreateQueue(q)
			assert.NoError(t, err)
			_, err = s.jobRepository.AddJobs([]*api.Job{job})
			assert.NoError(t, err)

			principal := authorization.NewStaticPrincipal("alice", []string{"reprioritize-jobs-group", "reprioritize-queue-group"})
			ctx := authorization.WithPrincipal(context.Background(), principal)

			_, err = s.ReprioritizeJobs(ctx, &api.JobReprioritizeRequest{
				Queue:    "test-queue",
				JobSetId: "job-set-1",
			})
			e, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.OK, e.Code())
		})
	})
}

func createJobRequest(jobSetId string, numberOfJobs int) *api.JobSubmitRequest {
	return &api.JobSubmitRequest{
		JobSetId:        jobSetId,
		Queue:           "test",
		JobRequestItems: createJobRequestItems(numberOfJobs),
	}
}

func createJobRequestItems(numberOfJobs int) []*api.JobSubmitRequestItem {
	cpu, _ := resource.ParseQuantity("1")
	memory, _ := resource.ParseQuantity("512Mi")

	jobRequestItems := make([]*api.JobSubmitRequestItem, 0, numberOfJobs)

	for i := 0; i < numberOfJobs; i++ {
		item := &api.JobSubmitRequestItem{
			ClientId: util.NewULID(),
			PodSpecs: []*v1.PodSpec{{
				Containers: []v1.Container{
					{
						Name:  fmt.Sprintf("Container %d", i),
						Image: "index.docker.io/library/ubuntu:latest",
						Args:  []string{"sleep", "10s"},
						Resources: v1.ResourceRequirements{
							Limits:   v1.ResourceList{"cpu": cpu, "memory": memory},
							Requests: v1.ResourceList{"cpu": cpu, "memory": memory},
						},
					},
				},
				NodeSelector: make(map[string]string),
			}},
			Priority: 0,
		}
		jobRequestItems = append(jobRequestItems, item)

	}

	return jobRequestItems
}

func withSubmitServer(action func(s *SubmitServer, events *repository.TestEventStore)) {
	withSubmitServerAndRepos(func(s *SubmitServer, jobRepo repository.JobRepository, events *repository.TestEventStore) {
		action(s, events)
	})
}

func withSubmitServerAndRepos(action func(s *SubmitServer, jobRepo repository.JobRepository, events *repository.TestEventStore)) {
	// using real redis instance as miniredis does not support streams
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 10})

	jobRepo := repository.NewRedisJobRepository(client)
	queueRepo := repository.NewRedisQueueRepository(client)
	schedulingInfoRepository := repository.NewRedisSchedulingInfoRepository(client)
	eventStore := &repository.TestEventStore{}

	queueConfig := configuration.QueueManagementConfig{DefaultPriorityFactor: 1}
	schedulingConfig := configuration.SchedulingConfig{
		DefaultJobTolerations: []v1.Toleration{
			{
				Key:      "default",
				Operator: v1.TolerationOpEqual,
				Value:    "true",
				Effect:   v1.TaintEffectNoSchedule,
			},
		},
		DefaultJobLimits: armadaresource.ComputeResources{
			"cpu":    resource.MustParse("1"),
			"memory": resource.MustParse("1Gi"),
		},
		MaxPodSpecSizeBytes: 65535,
		Preemption: configuration.PreemptionConfig{
			DefaultPriorityClass: "high",
			PriorityClasses:      map[string]schedulertypes.PriorityClass{"high": {0, false, nil, nil}},
		},
		MinTerminationGracePeriod: time.Duration(30 * time.Second),
		MaxTerminationGracePeriod: time.Duration(300 * time.Second),
	}

	server := NewSubmitServer(
		&FakePermissionChecker{},
		jobRepo,
		queueRepo,
		eventStore,
		schedulingInfoRepository,
		200,
		&queueConfig,
		&schedulingConfig)

	_, _ = client.FlushDB().Result()

	err := queueRepo.CreateQueue(queue.Queue{Name: "test", PriorityFactor: queue.PriorityFactor(1.0)})
	if err != nil {
		panic(err)
	}

	err = schedulingInfoRepository.UpdateClusterSchedulingInfo(&api.ClusterSchedulingInfoReport{
		ClusterId:  "test-cluster",
		ReportTime: time.Now(),
		NodeTypes: []*api.NodeType{{
			AllocatableResources: armadaresource.ComputeResources{"cpu": resource.MustParse("100"), "memory": resource.MustParse("100Gi")},
		}},
	})
	if err != nil {
		panic(err)
	}

	action(server, jobRepo, eventStore)
	_, _ = client.FlushDB().Result()
}

func TestSubmitServer_CreateJobs_WithJobIdReplacement(t *testing.T) {
	timeNow := time.Now()
	mockNow := func() time.Time {
		return timeNow
	}
	mockNewULID := func() string {
		return "test-ulid"
	}

	expected := []*api.Job{
		{
			Id:       "test-ulid",
			ClientId: "0",
			Queue:    "test",
			JobSetId: "test-jobsetid",

			Namespace: "test",
			Labels: map[string]string{
				"a.label": "job-id-is-test-ulid",
			},
			Annotations: map[string]string{
				"a.nnotation": "job-id-is-test-ulid",
			},

			Priority: 1,

			Created: mockNow(),
			PodSpecs: []*v1.PodSpec{
				{
					Containers: []v1.Container{
						{
							Name:  "app",
							Image: "test:latest",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									"cpu":    resource.MustParse("1"),
									"memory": resource.MustParse("100Mi"),
								},
								Requests: v1.ResourceList{
									"cpu":    resource.MustParse("1"),
									"memory": resource.MustParse("100Mi"),
								},
							},
						},
					},
					Tolerations: []v1.Toleration{
						{
							Key:      "default",
							Operator: "Equal",
							Value:    "true",
							Effect:   "NoSchedule",
						},
					},
					TerminationGracePeriodSeconds: pointer.Int64(30),
					PriorityClassName:             "high",
				},
			},
			Owner:                              "test",
			QueueOwnershipUserGroups:           nil,
			CompressedQueueOwnershipUserGroups: []byte{},
		},
	}

	// Empty - no response items expected as no jobs were submitted without errors
	var expectedResponseItems []*api.JobSubmitResponseItem

	request := &api.JobSubmitRequest{
		Queue:    "test",
		JobSetId: "test-jobsetid",
		JobRequestItems: []*api.JobSubmitRequestItem{
			{
				Priority:  1,
				Namespace: "test",
				ClientId:  "0",
				Labels: map[string]string{
					"a.label": "job-id-is-{JobId}",
				},
				Annotations: map[string]string{
					"a.nnotation": "job-id-is-{JobId}",
				},
				PodSpecs: []*v1.PodSpec{
					{
						Containers: []v1.Container{
							{
								Name:  "app",
								Image: "test:latest",
								Resources: v1.ResourceRequirements{
									Limits: v1.ResourceList{
										"cpu":    resource.MustParse("1"),
										"memory": resource.MustParse("100Mi"),
									},
									Requests: v1.ResourceList{
										"cpu":    resource.MustParse("1"),
										"memory": resource.MustParse("100Mi"),
									},
								},
							},
						},
					},
				},
			},
		},
	}
	ownershipGroups := make([]string, 0)
	withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
		output, responseItems, err := s.createJobsObjects(request, "test", ownershipGroups, mockNow, mockNewULID)
		assert.NoError(t, err)
		assert.Equal(t, expectedResponseItems, responseItems)
		assert.Equal(t, expected, output)
	})
}

func TestSubmitServer_CreateJobs_WithDuplicatePodSpec(t *testing.T) {
	timeNow := time.Now()
	mockNow := func() time.Time {
		return timeNow
	}
	mockNewULID := func() string {
		return "test-ulid"
	}

	expectedResponseItems := []*api.JobSubmitResponseItem{
		{
			JobId: "test-ulid",
			Error: "[createJobs] job 0 in job set test-jobsetid contains both podSpec and podSpecs, but may only contain either",
		},
	}
	expectedError := "[createJobs] error creating jobs, check JobSubmitResponse for details"

	request := &api.JobSubmitRequest{
		Queue:    "test",
		JobSetId: "test-jobsetid",
		JobRequestItems: []*api.JobSubmitRequestItem{
			{
				Priority:  1,
				Namespace: "test",
				ClientId:  "0",
				Labels: map[string]string{
					"a.label": "job-id-is-{JobId}",
				},
				Annotations: map[string]string{
					"a.nnotation": "job-id-is-{JobId}",
				},
				PodSpecs: []*v1.PodSpec{
					{
						Containers: []v1.Container{
							{
								Name:  "app",
								Image: "test:latest",
								Resources: v1.ResourceRequirements{
									Limits: v1.ResourceList{
										"cpu":    resource.MustParse("1"),
										"memory": resource.MustParse("100Mi"),
									},
									Requests: v1.ResourceList{
										"cpu":    resource.MustParse("1"),
										"memory": resource.MustParse("100Mi"),
									},
								},
							},
						},
					},
				},
				PodSpec: &v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "app",
							Image: "test:latest",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									"cpu":    resource.MustParse("1"),
									"memory": resource.MustParse("100Mi"),
								},
								Requests: v1.ResourceList{
									"cpu":    resource.MustParse("1"),
									"memory": resource.MustParse("100Mi"),
								},
							},
						},
					},
				},
			},
		},
	}
	ownershipGroups := make([]string, 0)
	withSubmitServer(func(s *SubmitServer, events *repository.TestEventStore) {
		output, responseItems, err := s.createJobsObjects(request, "test", ownershipGroups, mockNow, mockNewULID)
		assert.Equal(t, expectedError, err.Error())
		assert.Equal(t, expectedResponseItems, responseItems)
		assert.Nil(t, output)
	})
}
