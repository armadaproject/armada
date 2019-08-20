package service

import (
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/client/domain"
	"github.com/G-Research/k8s-batch/internal/client/util"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	v1 "k8s.io/api/core/v1"
	"strconv"
	"sync"
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

func (apiLoadTester ArmadaLoadTester) RunSubmissionTest(spec domain.LoadTestSpecification) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	for _, submission := range spec.Submissions {
		for i := 0; i < submission.Count; i++ {
			queue := submission.Queue
			if queue == "" {
				queue = submission.UserNamePrefix + "-" + strconv.Itoa(i)
			}
			jobSetId := queue + "-" + strconv.Itoa(i)
			wg.Add(1)
			go func() {
				defer wg.Done()
				apiLoadTester.runSubmission(queue, jobSetId, submission.Jobs)
			}()
		}
	}

	wg.Done()
	wg.Wait()
}

func (apiLoadTester ArmadaLoadTester) runSubmission(queue string, jobSetId string, jobs []*domain.JobSubmissionDescription) {
	util.WithConnection(apiLoadTester.apiConnectionDetails, func(connection *grpc.ClientConn) {
		client := api.NewSubmitClient(connection)
		timeout := util.DefaultTimeout()

		_, e := client.CreateQueue(timeout, &api.Queue{Name: queue, Priority: 1})
		if e != nil {
			log.Error(e)
			return
		}
		log.Infof("Queue %s created.", queue)

		for _, job := range jobs {
			jobRequest := createJobRequest(queue, jobSetId, job.Spec)
			for i := 0; i < job.Count; i++ {
				response, e := client.SubmitJob(timeout, jobRequest)

				if e != nil {
					log.Error(e)
					break
				}
				log.Infof("Submitted job id: %s (set: %s)", response.JobId, jobSetId)
			}
		}
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
