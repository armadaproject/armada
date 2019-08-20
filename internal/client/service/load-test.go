package service

import (
	"context"
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/client/domain"
	"github.com/G-Research/k8s-batch/internal/common"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	v1 "k8s.io/api/core/v1"
	"strconv"
	"sync"
	"time"
)

type LoadTester interface {
	RunSubmissionTest(spec domain.LoadTestSpecification)
}

type ArmadaLoadTester struct {
	apiUrl      string
	credentials common.LoginCredentials
}

func NewArmadaLoadTester(url string, credentials common.LoginCredentials) *ArmadaLoadTester {
	return &ArmadaLoadTester{
		apiUrl:      url,
		credentials: credentials,
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
	withConnection(apiLoadTester.apiUrl, &apiLoadTester.credentials, func(connection *grpc.ClientConn) {
		client := api.NewSubmitClient(connection)
		ctx := timeout()

		_, e := client.CreateQueue(timeout(), &api.Queue{Name: queue, Priority: 1})
		if e != nil {
			log.Error(e)
			return
		}
		log.Infof("Queue %s created.", queue)

		for _, job := range jobs {
			jobRequest := createJobRequest(queue, jobSetId, job.Spec)
			for i := 0; i < job.Count; i++ {
				response, e := client.SubmitJob(ctx, jobRequest)

				if e != nil {
					log.Error(e)
					break
				}
				log.Infof("Submitted job id: %s (set: %s)", response.JobId, jobSetId)
			}
		}
	})
}

func timeout() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	return ctx
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

func withConnection(url string, creds *common.LoginCredentials, action func(*grpc.ClientConn)) {
	conn, err := createConnection(url, creds)

	if err != nil {
		log.Errorf("Failed to connect to api because %s", err)
	}
	defer conn.Close()

	action(conn)
}

func createConnection(url string, creds *common.LoginCredentials) (*grpc.ClientConn, error) {
	if creds.Username == "" || creds.Password == "" {
		return grpc.Dial(url, grpc.WithInsecure())
	} else {
		return grpc.Dial(
			url,
			grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(nil, "")),
			grpc.WithPerRPCCredentials(creds))
	}
}
