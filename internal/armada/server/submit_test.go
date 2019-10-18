package server

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/armada/api"
	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/common/util"
)

func TestSubmitServer_SubmitJob(t *testing.T) {
	withSubmitServer(func(s *SubmitServer) {
		jobSetId := util.NewULID()
		jobRequest := createJobRequest(jobSetId)

		response, err := s.SubmitJob(context.Background(), jobRequest)

		assert.Empty(t, err)
		assert.NotNil(t, response.JobId)
	})
}

func TestSubmitServer_SubmitJob_AddsExpectedEventsInCorrectOrder(t *testing.T) {
	withSubmitServer(func(s *SubmitServer) {
		jobSetId := util.NewULID()
		jobRequest := createJobRequest(jobSetId)

		_, err := s.SubmitJob(context.Background(), jobRequest)
		assert.Empty(t, err)

		messages, err := s.eventRepository.ReadEvents(jobSetId, "", 100, 5*time.Second)

		assert.Empty(t, err)
		assert.Equal(t, len(messages), 2)

		//Sort events based on Redis stream ID order (Actual stored order)
		sort.Slice(messages, func(i, j int) bool {
			return messages[i].Id < messages[j].Id
		})

		firstEvent := messages[0]
		secondEvent := messages[1]

		//First event should be submitted
		assert.NotNil(t, firstEvent.Message.GetSubmitted())
		//Second event should be queued
		assert.NotNil(t, secondEvent.Message.GetQueued())
	})
}

func createJobRequest(jobSetId string) *api.JobRequest {
	cpu, _ := resource.ParseQuantity("1")
	memory, _ := resource.ParseQuantity("512Mi")
	return &api.JobRequest{
		PodSpec: &v1.PodSpec{
			Containers: []v1.Container{{
				Name:  "Container1",
				Image: "index.docker.io/library/ubuntu:latest",
				Args:  []string{"sleep", "10s"},
				Resources: v1.ResourceRequirements{
					Limits: v1.ResourceList{"cpu": cpu, "memory": memory},
				},
			},
			},
		},
		JobSetId: jobSetId,
		Priority: 0,
		Queue:    "test",
	}
}

func withSubmitServer(action func(s *SubmitServer)) {
	// using real redis instance as miniredis does not support streams
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 10})

	jobRepo := repository.NewRedisJobRepository(client)
	queueRepo := repository.NewRedisQueueRepository(client)
	eventRepo := repository.NewRedisEventRepository(client)
	server := NewSubmitServer(jobRepo, queueRepo, eventRepo)

	action(server)
}
