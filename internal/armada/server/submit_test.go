package server

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/pkg/api"
)

func TestSubmitServer_CreateQueue_WithDefaultSettings_CanBeReadBack(t *testing.T) {
	withSubmitServer(func(s *SubmitServer, events repository.EventRepository) {
		const queueName = "myQueue"
		const priority = 1.0

		_, err := s.CreateQueue(context.Background(), &api.Queue{Name: queueName, PriorityFactor: priority})
		assert.NoError(t, err)

		receivedQueue, err := s.GetQueue(context.Background(), &api.QueueGetRequest{Name: queueName})
		assert.NoError(t, err)
		assert.Equal(t, &api.Queue{Name: queueName, PriorityFactor: priority, UserOwners: []string{"anonymous"}, GroupOwners: nil, ResourceLimits: nil}, receivedQueue)
	})
}

func TestSubmitServer_CreateQueue_WithCustomSettings_CanBeReadBack(t *testing.T) {
	withSubmitServer(func(s *SubmitServer, events repository.EventRepository) {
		const queueName = "myQueue"
		originalQueue := &api.Queue{Name: queueName, PriorityFactor: 1.1, UserOwners: []string{"user-a", "user-b"}, GroupOwners: []string{"group-a", "group-b"}, ResourceLimits: map[string]float64{"user-a": 1.2, "user-b": 1.3}}

		_, err := s.CreateQueue(context.Background(), originalQueue)
		assert.NoError(t, err)

		roundTrippedQueue, err := s.GetQueue(context.Background(), &api.QueueGetRequest{Name: queueName})
		assert.NoError(t, err)
		assert.Equal(t, originalQueue, roundTrippedQueue)
	})
}

func TestSubmitServer_CreateQueue_WhenQueueAlreadyExists_QueueIsNotChanged_AndReturnsAlreadyExists(t *testing.T) {
	withSubmitServer(func(s *SubmitServer, events repository.EventRepository) {
		const queueName = "myQueue"
		originalQueue := &api.Queue{Name: queueName, PriorityFactor: 1.1, UserOwners: []string{"user-a", "user-b"}, GroupOwners: []string{"group-a", "group-b"}, ResourceLimits: map[string]float64{"user-a": 1.2, "user-b": 1.3}}

		_, err := s.CreateQueue(context.Background(), originalQueue)
		assert.NoError(t, err)

		_, err = s.CreateQueue(context.Background(), &api.Queue{Name: queueName, PriorityFactor: 2, UserOwners: []string{"user-c"}, GroupOwners: []string{"group-c"}, ResourceLimits: map[string]float64{"user-c": 1.4}})
		assert.Equal(t, codes.AlreadyExists, status.Code(err))

		roundTrippedQueue, err := s.GetQueue(context.Background(), &api.QueueGetRequest{Name: queueName})
		assert.NoError(t, err)
		assert.Equal(t, originalQueue, roundTrippedQueue)
	})
}

func TestSubmitServer_UpdateQueue_WhenQueueDoesNotExist_DoesNotCreateQueue_AndReturnsNotFound(t *testing.T) {
	withSubmitServer(func(s *SubmitServer, events repository.EventRepository) {
		const queueName = "non_existent_queue"

		_, err := s.UpdateQueue(context.Background(), &api.Queue{Name: queueName, PriorityFactor: 1})
		assert.Equal(t, codes.NotFound, status.Code(err))

		_, err = s.GetQueue(context.Background(), &api.QueueGetRequest{Name: queueName})
		assert.Equal(t, codes.NotFound, status.Code(err))
	})
}

func TestSubmitServer_UpdateQueue_WhenQueueExists_ReplacesQueue(t *testing.T) {
	withSubmitServer(func(s *SubmitServer, events repository.EventRepository) {
		const queueName = "myQueue"

		originalQueue := &api.Queue{Name: queueName, PriorityFactor: 1.1, UserOwners: []string{"user-a", "user-b"}, GroupOwners: []string{"group-a", "group-b"}, ResourceLimits: map[string]float64{"user-a": 1.2, "user-b": 1.3}}
		_, err := s.CreateQueue(context.Background(), originalQueue)
		assert.NoError(t, err)

		updatedQueue := &api.Queue{Name: queueName, PriorityFactor: 2.2, UserOwners: []string{"user-a", "user-c"}, GroupOwners: []string{"group-c", "group-b"}, ResourceLimits: map[string]float64{"user-a": 1.3, "user-c": 1.3}}
		_, err = s.UpdateQueue(context.Background(), updatedQueue)
		assert.NoError(t, err)

		receivedQueue, err := s.GetQueue(context.Background(), &api.QueueGetRequest{Name: queueName})
		assert.NoError(t, err)
		assert.Equal(t, updatedQueue, receivedQueue)
	})
}

func TestSubmitServer_CreateQueue_WhenPermissionsCheckFails_QueueIsNotCreated_AndReturnsPermissionDenied(t *testing.T) {
	withSubmitServer(func(s *SubmitServer, events repository.EventRepository) {
		const queueName = "myQueue"

		s.permissions = &FakeDenyAllPermissionChecker{}

		_, err := s.CreateQueue(context.Background(), &api.Queue{Name: queueName, PriorityFactor: 1})
		assert.Equal(t, codes.PermissionDenied, status.Code(err))

		_, err = s.GetQueue(context.Background(), &api.QueueGetRequest{Name: queueName})
		assert.Equal(t, codes.NotFound, status.Code(err))
	})
}

func TestSubmitServer_UpdateQueue_WhenPermissionsCheckFails_QueueIsNotUpdated_AndReturnsPermissionDenied(t *testing.T) {
	withSubmitServer(func(s *SubmitServer, events repository.EventRepository) {
		const queueName = "myQueue"
		originalQueue := &api.Queue{Name: queueName, PriorityFactor: 1}

		_, err := s.CreateQueue(context.Background(), originalQueue)
		assert.NoError(t, err)

		s.permissions = &FakeDenyAllPermissionChecker{}

		_, err = s.UpdateQueue(context.Background(), &api.Queue{Name: queueName, PriorityFactor: originalQueue.PriorityFactor + 100})
		assert.Equal(t, codes.PermissionDenied, status.Code(err))

		receivedQueue, err := s.GetQueue(context.Background(), &api.QueueGetRequest{Name: queueName})
		assert.NoError(t, err)
		assert.Equal(t, originalQueue, receivedQueue)
	})
}

func TestSubmitServer_DeleteQueue_WhenPermissionsCheckFails_QueueIsNotDelete_AndReturnsPermissionDenied(t *testing.T) {
	withSubmitServer(func(s *SubmitServer, events repository.EventRepository) {
		const queueName = "myQueue"
		originalQueue := &api.Queue{Name: queueName, PriorityFactor: 1}

		_, err := s.CreateQueue(context.Background(), originalQueue)
		assert.NoError(t, err)

		s.permissions = &FakeDenyAllPermissionChecker{}

		_, err = s.DeleteQueue(context.Background(), &api.QueueDeleteRequest{Name: queueName})
		assert.Equal(t, codes.PermissionDenied, status.Code(err))

		receivedQueue, err := s.GetQueue(context.Background(), &api.QueueGetRequest{Name: queueName})
		assert.NoError(t, err)
		assert.Equal(t, originalQueue, receivedQueue)
	})
}

func TestSubmitServer_GetQueueInfo_WhenPermissionsCheckFails_ReturnsPermissionDenied(t *testing.T) {
	withSubmitServer(func(s *SubmitServer, events repository.EventRepository) {
		const queueName = "myQueue"
		originalQueue := &api.Queue{Name: queueName, PriorityFactor: 1}

		_, err := s.CreateQueue(context.Background(), originalQueue)
		assert.NoError(t, err)

		s.permissions = &FakeDenyAllPermissionChecker{}

		receivedQueueInfo, err := s.GetQueueInfo(context.Background(), &api.QueueInfoRequest{Name: queueName})
		assert.Equal(t, codes.PermissionDenied, status.Code(err))
		assert.Empty(t, receivedQueueInfo)

	})
}

func TestSubmitServer_SubmitJob(t *testing.T) {
	withSubmitServer(func(s *SubmitServer, events repository.EventRepository) {
		jobSetId := util.NewULID()
		jobRequest := createJobRequest(jobSetId, 1)

		response, err := s.SubmitJobs(context.Background(), jobRequest)

		assert.Empty(t, err)
		assert.NotNil(t, response.JobResponseItems[0].JobId)
	})
}

func TestSubmitServer_SubmitJob_ApplyDefaults(t *testing.T) {

	withSubmitServer(func(s *SubmitServer, events repository.EventRepository) {
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

		assert.Equal(t, expectedResources, retrievedJob[0].PodSpecs[0].Containers[0].Resources.Requests)
		assert.Equal(t, expectedResources, retrievedJob[0].PodSpecs[0].Containers[0].Resources.Limits)
		assert.Equal(t, expectedTolerations, retrievedJob[0].PodSpecs[0].Tolerations)
	})
}

func TestSubmitServer_SubmitJob_RejectEmptyPodSpec(t *testing.T) {
	withSubmitServer(func(s *SubmitServer, events repository.EventRepository) {
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
	withSubmitServer(func(s *SubmitServer, events repository.EventRepository) {
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
	withSubmitServer(func(s *SubmitServer, events repository.EventRepository) {
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
	withSubmitServer(func(s *SubmitServer, events repository.EventRepository) {
		jobSetId := util.NewULID()
		jobRequest := createJobRequest(jobSetId, 1)

		err := s.schedulingInfoRepository.UpdateClusterSchedulingInfo(&api.ClusterSchedulingInfoReport{
			ClusterId:  "test-cluster",
			ReportTime: time.Now(),
			NodeTypes: []*api.NodeType{{
				Taints:               nil,
				Labels:               nil,
				AllocatableResources: common.ComputeResources{"cpu": resource.MustParse("0"), "memory": resource.MustParse("0")},
			}},
		})
		assert.Empty(t, err)

		_, err = s.SubmitJobs(context.Background(), jobRequest)

		assert.Error(t, err)
	})
}

func TestSubmitServer_SubmitJob_AddsExpectedEventsInCorrectOrder(t *testing.T) {
	withSubmitServer(func(s *SubmitServer, events repository.EventRepository) {
		jobSetId := util.NewULID()
		jobRequest := createJobRequest(jobSetId, 1)

		_, err := s.SubmitJobs(context.Background(), jobRequest)
		assert.Empty(t, err)

		messages, err := readJobEvents(events, jobSetId)
		assert.NoError(t, err)
		assert.Equal(t, len(messages), 2)

		firstEvent := messages[0]
		secondEvent := messages[1]

		//First event should be submitted
		assert.NotNil(t, firstEvent.Message.GetSubmitted())
		//Second event should be queued
		assert.NotNil(t, secondEvent.Message.GetQueued())
	})
}

func TestSubmitServer_SubmitJob_ReturnsJobItemsInTheSameOrderTheyWereSubmitted(t *testing.T) {
	withSubmitServer(func(s *SubmitServer, events repository.EventRepository) {
		jobSetId := util.NewULID()
		jobRequest := createJobRequest(jobSetId, 5)

		response, err := s.SubmitJobs(context.Background(), jobRequest)
		assert.Empty(t, err)

		jobIds := make([]string, 0, 5)

		for _, jobItem := range response.JobResponseItems {
			jobIds = append(jobIds, jobItem.JobId)
		}

		//Get jobs for jobIds returned
		jobs, _ := s.jobRepository.GetExistingJobsByIds(jobIds)
		jobSet := make(map[string]*api.Job, 5)
		for _, job := range jobs {
			jobSet[job.Id] = job
		}

		//Confirm submitted spec and created spec line up, using order of returned jobIds to correlate submitted to created
		for i := 0; i < len(jobRequest.JobRequestItems); i++ {
			requestItem := jobRequest.JobRequestItems[i]
			returnedId := jobIds[i]
			createdJob := jobSet[returnedId]

			assert.NotNil(t, createdJob)
			assert.Equal(t, requestItem.PodSpec, createdJob.PodSpec)
		}
	})
}

func TestSubmitServer_SubmitJobs_HandlesDoubleSubmit(t *testing.T) {
	withSubmitServer(func(s *SubmitServer, events repository.EventRepository) {
		jobSetId := util.NewULID()
		jobRequest := createJobRequest(jobSetId, 1)

		result, err := s.SubmitJobs(context.Background(), jobRequest)
		assert.NoError(t, err)

		result2, err := s.SubmitJobs(context.Background(), jobRequest)
		assert.NoError(t, err)

		assert.Equal(t, result.JobResponseItems[0].JobId, result2.JobResponseItems[0].JobId)

		messages, err := readJobEvents(events, jobSetId)
		assert.NoError(t, err)
		assert.Equal(t, len(messages), 4)

		submitted := messages[0].Message.GetSubmitted()
		queued := messages[1].Message.GetQueued()
		submitted2 := messages[2].Message.GetSubmitted()
		duplicateFound := messages[3].Message.GetDuplicateFound()

		assert.NotNil(t, submitted)
		assert.NotNil(t, queued)
		assert.NotNil(t, submitted2)
		assert.NotNil(t, duplicateFound)

		assert.Equal(t, duplicateFound.OriginalJobId, submitted.JobId)
		assert.Equal(t, duplicateFound.JobId, submitted2.JobId)
	})
}

func TestSubmitServer_ReprioritizeJobs(t *testing.T) {
	t.Run("job that doesn't exist", func(t *testing.T) {
		withSubmitServerAndRepos(func(s *SubmitServer, jobRepo repository.JobRepository, events repository.EventRepository) {
			reprioritizeResponse, err := s.ReprioritizeJobs(context.Background(), &api.JobReprioritizeRequest{
				JobIds:      []string{util.NewULID()},
				NewPriority: 123,
			})
			assert.NoError(t, err)
			assert.Equal(t, 0, len(reprioritizeResponse.ReprioritizationResults))
		})
	})

	t.Run("one job", func(t *testing.T) {
		withSubmitServerAndRepos(func(s *SubmitServer, jobRepo repository.JobRepository, events repository.EventRepository) {

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

			messages, err := readJobEvents(events, jobSetId)
			assert.NoError(t, err)
			assert.Equal(t, 5, len(messages))

			assert.NotNil(t, messages[0].Message.GetSubmitted())
			assert.NotNil(t, messages[1].Message.GetQueued())
			assert.NotNil(t, messages[2].Message.GetReprioritizing())
			assert.NotNil(t, messages[3].Message.GetUpdated())
			assert.NotNil(t, messages[4].Message.GetReprioritized())

			assert.Equal(t, newPriority, messages[3].Message.GetUpdated().Job.Priority)
			assert.Equal(t, newPriority, messages[4].Message.GetReprioritized().NewPriority)
		})
	})

	t.Run("multiple jobs", func(t *testing.T) {
		withSubmitServerAndRepos(func(s *SubmitServer, jobRepo repository.JobRepository, events repository.EventRepository) {
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

			messages, err := readJobEvents(events, jobSetId)
			assert.NoError(t, err)
			assert.Equal(t, 5*3, len(messages))
		})
	})

	t.Run("leased job", func(t *testing.T) {
		withSubmitServerAndRepos(func(s *SubmitServer, jobRepo repository.JobRepository, events repository.EventRepository) {
			jobSetId := util.NewULID()
			jobRequest := createJobRequest(jobSetId, 1)

			submitResult, err := s.SubmitJobs(context.Background(), jobRequest)
			assert.NoError(t, err)
			jobId := submitResult.JobResponseItems[0].JobId

			jobs, err := s.jobRepository.GetExistingJobsByIds([]string{jobId})
			assert.NoError(t, err)
			leased, err := jobRepo.TryLeaseJobs("some-cluster", "test", jobs)
			assert.NoError(t, err)
			assert.Equal(t, 1, len(leased))

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

			messages, err := readJobEvents(events, jobSetId)
			assert.NoError(t, err)
			assert.Equal(t, 5, len(messages))

			assert.NotNil(t, messages[0].Message.GetSubmitted())
			assert.NotNil(t, messages[1].Message.GetQueued())
			assert.NotNil(t, messages[2].Message.GetReprioritizing())
			assert.NotNil(t, messages[3].Message.GetUpdated())
			assert.NotNil(t, messages[4].Message.GetReprioritized())
		})
	})

	t.Run("all jobs in a job set", func(t *testing.T) {
		withSubmitServerAndRepos(func(s *SubmitServer, jobRepo repository.JobRepository, events repository.EventRepository) {
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

			messages, err := readJobEvents(events, jobSetId)
			assert.NoError(t, err)
			assert.Equal(t, 5*3, len(messages))
		})
	})

	t.Run("updating priority after lease keeps priority", func(t *testing.T) {
		withSubmitServerAndRepos(func(s *SubmitServer, jobRepo repository.JobRepository, events repository.EventRepository) {
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

			leased, err := jobRepo.TryLeaseJobs(clusterId, "test", []*api.Job{selectedJob})
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

func readJobEvents(events repository.EventRepository, jobSetId string) ([]*api.EventStreamMessage, error) {
	messages, err := events.ReadEvents("test", jobSetId, "", 100, 5*time.Second)
	if err != nil {
		return nil, err
	}

	//Sort events based on Redis stream ID order (Actual stored order)
	sort.Slice(messages, func(i, j int) bool {
		return messages[i].Id < messages[j].Id
	})
	return messages, nil
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
			}},
			Priority: 0,
		}
		jobRequestItems = append(jobRequestItems, item)

	}

	return jobRequestItems
}

func withSubmitServer(action func(s *SubmitServer, events repository.EventRepository)) {
	withSubmitServerAndRepos(func(s *SubmitServer, jobRepo repository.JobRepository, events repository.EventRepository) {
		action(s, events)
	})
}

func withSubmitServerAndRepos(action func(s *SubmitServer, jobRepo repository.JobRepository, events repository.EventRepository)) {
	// using real redis instance as miniredis does not support streams
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 10})

	jobRepo := repository.NewRedisJobRepository(client, configuration.DatabaseRetentionPolicy{JobRetentionDuration: time.Hour})
	queueRepo := repository.NewRedisQueueRepository(client)
	eventRepo := repository.NewRedisEventRepository(client, configuration.EventRetentionPolicy{ExpiryEnabled: false})
	schedulingInfoRepository := repository.NewRedisSchedulingInfoRepository(client)

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
		DefaultJobLimits: common.ComputeResources{
			"cpu":    resource.MustParse("1"),
			"memory": resource.MustParse("1Gi"),
		},
		MaxPodSpecSizeBytes: 65535,
	}

	server := NewSubmitServer(
		&FakePermissionChecker{},
		jobRepo,
		queueRepo,
		eventRepo,
		schedulingInfoRepository,
		200,
		&queueConfig,
		&schedulingConfig)

	err := queueRepo.CreateQueue(&api.Queue{Name: "test"})
	if err != nil {
		panic(err)
	}

	err = schedulingInfoRepository.UpdateClusterSchedulingInfo(&api.ClusterSchedulingInfoReport{
		ClusterId:  "test-cluster",
		ReportTime: time.Now(),
		NodeTypes: []*api.NodeType{{
			AllocatableResources: common.ComputeResources{"cpu": resource.MustParse("100"), "memory": resource.MustParse("100Gi")},
		}},
	})
	if err != nil {
		panic(err)
	}

	action(server, jobRepo, eventRepo)
	_, _ = client.FlushDB().Result()
}

func TestSubmitServer_CreateJobs_WithJobIdReplacement(t *testing.T) {
	timeNow := time.Now()
	now = func() time.Time {
		return timeNow
	}

	NewULID = func() string {
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

			Created: now(),
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
				},
			},
			Owner:                    "test",
			QueueOwnershipUserGroups: []string{},
		},
	}

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
	owner := "test"
	ownershipGroups := make([]string, 0)
	withSubmitServer(func(s *SubmitServer, events repository.EventRepository) {
		output, err := s.createJobs(request, owner, ownershipGroups)
		fmt.Println(err)
		assert.Equal(t, expected, output)
	})

	// Replace mocked functions
	NewULID = util.NewULID
	now = time.Now
}
