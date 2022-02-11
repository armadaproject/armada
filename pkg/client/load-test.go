package client

import (
	"context"
	"strconv"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client/domain"
)

type LoadTester interface {
	RunSubmissionTest(ctx context.Context, spec domain.LoadTestSpecification, watchEvents bool) *domain.WatchContext
}

type ArmadaLoadTester struct {
	apiConnectionDetails *ApiConnectionDetails
}

func NewArmadaLoadTester(connectionDetails *ApiConnectionDetails) *ArmadaLoadTester {
	return &ArmadaLoadTester{
		apiConnectionDetails: connectionDetails,
	}
}

func (apiLoadTester ArmadaLoadTester) RunSubmissionTest(ctx context.Context, spec domain.LoadTestSpecification, watchEvents bool) domain.LoadTestSummary {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	eventChannel := make(chan api.Event, 10000)

	var jobCurrentState *domain.WatchContext
	if watchEvents {
		complete, cancel, currentState := watchJobInfoChannel(eventChannel)
		jobCurrentState = currentState
		defer complete.Wait()
		defer func() { cancel <- true }()
	}

	allSubmittedJobs := NewThreadSafeStringSlice()
	for _, submission := range spec.Submissions {
		for i := 0; i < submission.Count; i++ {
			wg.Add(1)
			go func(i int, submission *domain.SubmissionDescription) {
				defer wg.Done()
				jobIdChannel, jobSetId, submissionComplete := apiLoadTester.runSubmission(ctx, submission, i)
				if watchEvents {
					submittedIds := apiLoadTester.monitorJobsUntilCompletion(ctx, createQueueName(submission, i), jobSetId, jobIdChannel, eventChannel)
					allSubmittedJobs.Append(submittedIds)
				}
				if ctx.Err() != nil {
					apiLoadTester.cancelRemainingJobs(createQueueName(submission, i), jobSetId)
				}
				submissionComplete.Wait()
			}(i, submission)
		}
	}

	wg.Done()
	wg.Wait()

	return domain.LoadTestSummary{
		SubmittedJobs: allSubmittedJobs.GetAll(),
		CurrentState:  jobCurrentState,
	}
}

func watchJobInfoChannel(eventChannel chan api.Event) (*sync.WaitGroup, chan bool, *domain.WatchContext) {
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

	return complete, stop, aggregatedCurrentState
}

func (apiLoadTester ArmadaLoadTester) runSubmission(ctx context.Context, submission *domain.SubmissionDescription, i int) (jobIds chan string, jobSetId string, submissionComplete *sync.WaitGroup) {
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

	submissionComplete = &sync.WaitGroup{}
	submissionComplete.Add(1)

	go WithConnection(apiLoadTester.apiConnectionDetails, func(connection *grpc.ClientConn) {
		defer submissionComplete.Done()

		client := api.NewSubmitClient(connection)

		e := CreateQueue(client, &api.Queue{Name: queue, PriorityFactor: priorityFactor})
		if status.Code(e) == codes.AlreadyExists {
			log.Infof("Queue %s already exists so no need to create it.\n", queue)
		} else if e != nil {
			log.Errorf("ERROR: Failed to create queue: %s because: %s\n", queue, e)
			return
		} else {
			log.Infof("Queue %s created.\n", queue)
		}

		for len(jobs) > 0 {
			select {
			case <-ctx.Done():
				break
			default:
			}
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
	return jobIds, jobSetId, submissionComplete
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

func (apiLoadTester ArmadaLoadTester) monitorJobsUntilCompletion(ctx context.Context, queue, jobSetId string, jobIds chan string, eventChannel chan api.Event) []string {
	var submittedIds []string = nil
	go func() {
		ids := []string{}
		for id := range jobIds {
			ids = append(ids, id)
		}
		submittedIds = ids
	}()
	WithConnection(apiLoadTester.apiConnectionDetails, func(connection *grpc.ClientConn) {
		eventsClient := api.NewEventClient(connection)

		WatchJobSet(eventsClient, queue, jobSetId, true, false, ctx, func(state *domain.WatchContext, e api.Event) bool {
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
	return submittedIds
}

func createJobSubmitRequestItems(jobDescs []*domain.JobSubmissionDescription) []*api.JobSubmitRequestItem {
	requestItems := []*api.JobSubmitRequestItem{}
	for _, jobDesc := range jobDescs {
		for i := 0; i < jobDesc.Count; i++ {
			requestItems = append(requestItems, &api.JobSubmitRequestItem{
				Priority:           jobDesc.Priority,
				Namespace:          jobDesc.Namespace,
				Annotations:        jobDesc.Annotations,
				Labels:             jobDesc.Labels,
				RequiredNodeLabels: jobDesc.RequiredNodeLabels,
				PodSpec:            jobDesc.Spec,
			})
		}
	}
	return requestItems
}

func (apiLoadTester ArmadaLoadTester) cancelRemainingJobs(queue string, jobSetId string) {
	WithConnection(apiLoadTester.apiConnectionDetails, func(connection *grpc.ClientConn) {
		client := api.NewSubmitClient(connection)

		timeout, _ := common.ContextWithDefaultTimeout()
		cancelRequest := &api.JobCancelRequest{
			JobSetId: jobSetId,
			Queue:    queue,
		}
		_, _ = client.CancelJobs(timeout, cancelRequest)
	})
}

type threadSafeStringSlice struct {
	slice []string
	mutex *sync.Mutex
}

func NewThreadSafeStringSlice() *threadSafeStringSlice {
	return &threadSafeStringSlice{
		slice: make([]string, 0, 10),
		mutex: &sync.Mutex{},
	}
}

func (s *threadSafeStringSlice) Append(additionalElements []string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.slice = append(s.slice, additionalElements...)
}

func (s *threadSafeStringSlice) GetAll() []string {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return append([]string{}, s.slice...)
}
