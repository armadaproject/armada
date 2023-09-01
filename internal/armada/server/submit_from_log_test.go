package server

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/armada/repository"
	"github.com/armadaproject/armada/internal/common/ingest/testfixtures"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

var events = []*armadaevents.EventSequence_Event{
	{
		Created: &testfixtures.BaseTime,
		Event: &armadaevents.EventSequence_Event_JobRunRunning{
			JobRunRunning: &armadaevents.JobRunRunning{
				RunId: testfixtures.RunIdProto,
				JobId: testfixtures.JobIdProto,
				ResourceInfos: []*armadaevents.KubernetesResourceInfo{
					{
						Info: &armadaevents.KubernetesResourceInfo_PodInfo{
							PodInfo: &armadaevents.PodInfo{
								NodeName:  testfixtures.NodeName,
								PodNumber: testfixtures.PodNumber,
							},
						},
					},
				},
			},
		},
	},
}

func TestUpdateJobStartTimes(t *testing.T) {
	jobRepo := newMockJobRepository()
	s := SubmitFromLog{
		SubmitServer: &SubmitServer{
			jobRepository: jobRepo,
		},
	}

	ok, err := s.UpdateJobStartTimes(ctx.Background(), events)
	assert.NoError(t, err)
	assert.True(t, ok)

	jobRunInfo, exists := jobRepo.jobStartTimeInfos[testfixtures.JobIdString]
	assert.True(t, exists)
	assert.Equal(t, testfixtures.BaseTime.UTC(), jobRunInfo.StartTime.UTC())
}

func TestUpdateJobStartTimes_NonExistentJob(t *testing.T) {
	jobRepo := newMockJobRepository()
	jobRepo.updateJobStartTimeError = &repository.ErrJobNotFound{JobId: "jobId", ClusterId: "clusterId"}
	s := SubmitFromLog{
		SubmitServer: &SubmitServer{
			jobRepository: jobRepo,
		},
	}
	ok, err := s.UpdateJobStartTimes(ctx.Background(), events)
	assert.Nil(t, err)
	assert.True(t, ok)

	_, exists := jobRepo.jobStartTimeInfos[testfixtures.JobIdString]
	assert.False(t, exists)
}

func TestUpdateJobStartTimes_RedisError(t *testing.T) {
	jobRepo := newMockJobRepository()
	jobRepo.redisError = fmt.Errorf("redis error")
	s := SubmitFromLog{
		SubmitServer: &SubmitServer{
			jobRepository: jobRepo,
		},
	}
	ok, err := s.UpdateJobStartTimes(ctx.Background(), events)
	assert.Error(t, err)
	assert.False(t, ok)

	_, exists := jobRepo.jobStartTimeInfos[testfixtures.JobIdString]
	assert.False(t, exists)
}
