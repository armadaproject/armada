package service

import (
	"fmt"
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/client/domain"
	"github.com/G-Research/k8s-batch/internal/client/util"
	"google.golang.org/grpc"
	v1 "k8s.io/api/core/v1"
	"strconv"
	"sync"
	"time"
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

	jobInfoChannel := make(chan JobInfo, 1000)

	if watchEvents {
		complete, cancel := watchJobInfoChannel(jobInfoChannel)
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
					apiLoadTester.monitorJobsUntilCompletion(jobSetId, submittedJobIds, jobInfoChannel)
				}
			}(i, submission)
		}
	}

	wg.Done()
	wg.Wait()
}

func watchJobInfoChannel(jobInfoChannel chan JobInfo) (*sync.WaitGroup, chan bool) {
	stop := make(chan bool)
	tickChannel := time.NewTicker(5 * time.Second)

	complete := &sync.WaitGroup{}
	complete.Add(1)

	aggregatedCurrentState := make(map[string]*JobInfo)

	go func() {
		for {
			select {
			case jobInfo := <-jobInfoChannel:
				aggregatedCurrentState[jobInfo.Job.Id] = &jobInfo
			case <-tickChannel.C:
				fmt.Println(CreateSummaryOfCurrentState(aggregatedCurrentState))
			case <-stop:
				fmt.Println(CreateSummaryOfCurrentState(aggregatedCurrentState))
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
			fmt.Printf("ERROR: Failed to create queue: %s because: %s\n", queue, e)
			return
		}
		fmt.Printf("Queue %s created.\n", queue)

		for _, job := range jobs {
			jobRequest := createJobRequest(queue, jobSetId, job.Spec)
			for i := 0; i < job.Count; i++ {
				response, e := SubmitJob(client, jobRequest)

				if e != nil {
					fmt.Printf("ERROR: Failed to submit job for jobset: %s because %s\n", jobSetId, e)
					continue
				}
				fmt.Printf("Submitted job id: %s (set: %s)\n", response.JobId, jobSetId)
				jobIds = append(jobIds, response.JobId)
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
		fmt.Printf("ERROR: Queue name is blank, please set queue or queuePrefix ")
		panic("Queue name is blank")
	}
	return queue
}

func (apiLoadTester ArmadaLoadTester) monitorJobsUntilCompletion(jobSetId string, jobIds []string, jobInfoChannel chan JobInfo) {
	util.WithConnection(apiLoadTester.apiConnectionDetails, func(connection *grpc.ClientConn) {
		eventsClient := api.NewEventClient(connection)
		WatchJobSetWithJobIdsFilter(eventsClient, jobSetId, true, jobIds, func(state map[string]*JobInfo, e api.Event) bool {
			currentState := state[e.GetJobId()]
			jobInfoChannel <- *currentState

			stateCounts := CountStates(state)
			if stateCounts[Succeeded]+stateCounts[Failed]+stateCounts[Cancelled] == len(jobIds) {
				return true
			}

			return false
		})
	})
}

func createJobRequest(queue string, jobSetId string, spec *v1.PodSpec) *api.JobRequest {
	job := api.JobRequest{
		Priority: 1,
		Queue:    queue,
		JobSetId: jobSetId,
	}
	job.PodSpec = spec
	return &job
}
