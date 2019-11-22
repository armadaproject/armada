package client

import (
	"context"
	"strconv"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"github.com/G-Research/armada/internal/armada/api"
	"github.com/G-Research/armada/internal/client/domain"
	"github.com/G-Research/armada/internal/client/util"
)

type LoadTester interface {
	RunSubmissionTest(spec domain.LoadTestSpecification)
}

type ArmadaLoadTester struct {
	apiConnectionDetails *domain.ArmadaApiConnectionDetails
}

func NewArmadaLoadTester(connectionDetails *domain.ArmadaApiConnectionDetails) *ArmadaLoadTester {
	return &ArmadaLoadTester{
		apiConnectionDetails: connectionDetails,
	}
}

func (apiLoadTester ArmadaLoadTester) RunSubmissionTest(spec domain.LoadTestSpecification, watchEvents bool) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	eventChannel := make(chan api.Event, 10000)

	if watchEvents {
		complete, cancel := watchJobInfoChannel(eventChannel)
		defer complete.Wait()
		defer func() { cancel <- true }()
	}

	for _, submission := range spec.Submissions {
		for i := 0; i < submission.Count; i++ {
			wg.Add(1)
			go func(i int, submission *domain.SubmissionDescription) {
				defer wg.Done()
				submittedJobIds, jobSetId := apiLoadTester.runSubmission(submission, i)
				if watchEvents {
					apiLoadTester.monitorJobsUntilCompletion(jobSetId, submittedJobIds, eventChannel)
				}
			}(i, submission)
		}
	}

	wg.Done()
	wg.Wait()
}

func watchJobInfoChannel(eventChannel chan api.Event) (*sync.WaitGroup, chan bool) {
	stop := make(chan bool)
	tickChannel := time.NewTicker(5 * time.Second)

	complete := &sync.WaitGroup{}
	complete.Add(1)

	aggregatedCurrentState := domain.NewWatchContext()

	go func() {
		for {
			select {
			case event := <-eventChannel:
				aggregatedCurrentState.ProcessEvent(event)
			case <-tickChannel.C:
				log.Info(aggregatedCurrentState.GetCurrentStateSummary())
			case <-stop:
				log.Info(aggregatedCurrentState.GetCurrentStateSummary())
				complete.Done()
				return
			}
		}
	}()

	return complete, stop
}

func (apiLoadTester ArmadaLoadTester) runSubmission(submission *domain.SubmissionDescription, i int) (jobIds []string, jobSetId string) {
	queue := createQueueName(submission, i)

	priorityFactor := submission.QueuePriorityFactor
	if priorityFactor <= 0 {
		priorityFactor = 1
	}
	jobSetId = submission.JobSetPrefix + "-" + strconv.Itoa(i)
	jobs := submission.Jobs

	jobIds = make([]string, 0, len(jobs))
	util.WithConnection(apiLoadTester.apiConnectionDetails, func(connection *grpc.ClientConn) {
		client := api.NewSubmitClient(connection)

		e := CreateQueue(client, &api.Queue{Name: queue, PriorityFactor: priorityFactor})
		if e != nil {
			log.Errorf("ERROR: Failed to create queue: %s because: %s\n", queue, e)
			return
		}
		log.Infof("Queue %s created.\n", queue)

		for _, job := range jobs {
			jobRequestItems := createJobSubmitRequestItems(job)
			requests := CreateChunkedSubmitRequests(queue, jobSetId, jobRequestItems)

			for _, request := range requests {
				response, e := SubmitJobs(client, request)

				if e != nil {
					log.Errorf("ERROR: Failed to submit jobs for job set: %s because %s\n", jobSetId, e)
					continue
				}
				failedJobs := 0

				for _, jobSubmitResponse := range response.JobResponseItems {
					if jobSubmitResponse.Error != "" {
						failedJobs++
					} else {
						jobIds = append(jobIds, jobSubmitResponse.JobId)
					}
				}

				log.Infof("Submitted %d jobs to queue %s job set %s", len(request.JobRequestItems), queue, jobSetId)
				if failedJobs > 0 {
					log.Errorf("ERROR: %d jobs failed to be created when submitting to queue %s job set %s", failedJobs, queue, jobSetId)
				}
			}
		}
	})
	return jobIds, jobSetId
}

func createQueueName(submission *domain.SubmissionDescription, i int) string {
	queue := ""

	if submission.Queue != "" {
		queue = submission.Queue
	} else if submission.QueuePrefix != "" {
		queue = submission.QueuePrefix + "-" + strconv.Itoa(i)
	}

	if queue == "" {
		log.Error("ERROR: Queue name is blank, please set queue or queuePrefix ")
		panic("Queue name is blank")
	}
	return queue
}

func (apiLoadTester ArmadaLoadTester) monitorJobsUntilCompletion(jobSetId string, jobIds []string, eventChannel chan api.Event) {
	util.WithConnection(apiLoadTester.apiConnectionDetails, func(connection *grpc.ClientConn) {
		eventsClient := api.NewEventClient(connection)
		WatchJobSetWithJobIdsFilter(eventsClient, jobSetId, true, jobIds, context.Background(), func(state *domain.WatchContext, e api.Event) bool {
			eventChannel <- e

			numberOfJobsInCompletedState := state.GetNumberOfJobsInStates([]domain.JobStatus{domain.Succeeded, domain.Failed, domain.Cancelled})
			if numberOfJobsInCompletedState == len(jobIds) {
				return true
			}

			return false
		})
	})
}

func createJobSubmitRequestItems(jobDesc domain.JobSubmissionDescription) []*api.JobSubmitRequestItem {
	requestItems := make([]*api.JobSubmitRequestItem, 0, jobDesc.Count)
	job := api.JobSubmitRequestItem{
		Priority:  1,
		Namespace: jobDesc.Namespace,
		PodSpec:   jobDesc.Spec,
	}
	for i := 0; i < jobDesc.Count; i++ {
		requestItems = append(requestItems, &job)
	}

	return requestItems
}
