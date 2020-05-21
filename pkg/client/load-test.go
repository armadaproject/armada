package client

import (
	"context"
	"strconv"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client/domain"
)

type LoadTester interface {
	RunSubmissionTest(spec domain.LoadTestSpecification)
}

type ArmadaLoadTester struct {
	apiConnectionDetails *ApiConnectionDetails
}

func NewArmadaLoadTester(connectionDetails *ApiConnectionDetails) *ArmadaLoadTester {
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
				jobIdChannel, jobSetId := apiLoadTester.runSubmission(submission, i)
				if watchEvents {
					apiLoadTester.monitorJobsUntilCompletion(createQueueName(submission, i), jobSetId, jobIdChannel, eventChannel)
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
				close(eventChannel)
				for event := range eventChannel {
					aggregatedCurrentState.ProcessEvent(event)
				}
				log.Info(aggregatedCurrentState.GetCurrentStateSummary())
				complete.Done()
				return
			}
		}
	}()

	return complete, stop
}

func (apiLoadTester ArmadaLoadTester) runSubmission(submission *domain.SubmissionDescription, i int) (jobIds chan string, jobSetId string) {
	queue := createQueueName(submission, i)
	startTime := time.Now()

	priorityFactor := submission.QueuePriorityFactor
	if priorityFactor <= 0 {
		priorityFactor = 1
	}
	jobSetId = submission.JobSetPrefix + "-" + strconv.Itoa(i)
	jobs := submission.Jobs

	jobCount := 0
	for _, job := range jobs {
		jobCount += job.Count
	}

	jobIds = make(chan string, jobCount)

	go WithConnection(apiLoadTester.apiConnectionDetails, func(connection *grpc.ClientConn) {
		client := api.NewSubmitClient(connection)

		e := CreateQueue(client, &api.Queue{Name: queue, PriorityFactor: priorityFactor})
		if e != nil {
			log.Errorf("ERROR: Failed to create queue: %s because: %s\n", queue, e)
			return
		}
		log.Infof("Queue %s created.\n", queue)

		for len(jobs) > 0 {
			readyJobs, remainingJobs := filterReadyJobs(startTime, jobs)
			jobs = remainingJobs

			readyRequests := createJobSubmitRequestItems(readyJobs)
			requests := CreateChunkedSubmitRequests(queue, jobSetId, readyRequests)

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
						jobIds <- jobSubmitResponse.JobId
					}
				}

				log.Infof("Submitted %d jobs to queue %s job set %s", len(request.JobRequestItems), queue, jobSetId)
				if failedJobs > 0 {
					log.Errorf("ERROR: %d jobs failed to be created when submitting to queue %s job set %s", failedJobs, queue, jobSetId)
				}
			}

			if len(jobs) > 0 {
				time.Sleep(time.Second)
			}
		}
		close(jobIds)
	})
	return jobIds, jobSetId
}

func filterReadyJobs(startTime time.Time, jobs []*domain.JobSubmissionDescription) (ready []*domain.JobSubmissionDescription, notReady []*domain.JobSubmissionDescription) {
	now := time.Now()
	ready = []*domain.JobSubmissionDescription{}
	notReady = []*domain.JobSubmissionDescription{}
	for _, j := range jobs {
		if startTime.Add(j.DelaySubmit).Before(now) {
			ready = append(ready, j)
		} else {
			notReady = append(notReady, j)
		}
	}
	return ready, notReady
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

func (apiLoadTester ArmadaLoadTester) monitorJobsUntilCompletion(queue, jobSetId string, jobIds chan string, eventChannel chan api.Event) {
	WithConnection(apiLoadTester.apiConnectionDetails, func(connection *grpc.ClientConn) {
		eventsClient := api.NewEventClient(connection)

		var submittedIds []string = nil
		go func() {
			ids := []string{}
			for id := range jobIds {
				ids = append(ids, id)
			}
			submittedIds = ids
		}()

		WatchJobSet(eventsClient, queue, jobSetId, true, context.Background(), func(state *domain.WatchContext, e api.Event) bool {
			eventChannel <- e

			if submittedIds == nil {
				return false
			}

			numberOfJobsInCompletedState := state.GetNumberOfFinishedJobs()
			if numberOfJobsInCompletedState < len(submittedIds) {
				return false
			}

			return state.AreJobsFinished(submittedIds)
		})
	})
}

func createJobSubmitRequestItems(jobDescs []*domain.JobSubmissionDescription) []*api.JobSubmitRequestItem {
	requestItems := []*api.JobSubmitRequestItem{}
	for _, jobDesc := range jobDescs {
		job := api.JobSubmitRequestItem{
			Priority:           jobDesc.Priority,
			Namespace:          jobDesc.Namespace,
			Annotations:        jobDesc.Annotations,
			Labels:             jobDesc.Labels,
			RequiredNodeLabels: jobDesc.RequiredNodeLabels,
			PodSpec:            jobDesc.Spec,
		}
		for i := 0; i < jobDesc.Count; i++ {
			requestItems = append(requestItems, &job)
		}
	}
	return requestItems
}
