package service

import (
	"context"
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/client/domain"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	v1 "k8s.io/api/core/v1"
	"time"
)

type LoadTester interface {
	RunSubmissionTest(spec domain.LoadTestSpecification)
}

type ArmadaApiLoadTester struct {
	apiUrl string
}

func (apiLoadTester ArmadaApiLoadTester) RunSubmissionTest(spec domain.LoadTestSpecification) {
	for _, submission := range spec.Submission {
		for i := 0; i < submission.Count; i++ {
			queue := submission.Queue
			if queue == "" {
				queue = submission.UserNamePrefix + "-" + string(i)
			}
			jobSetId := queue + "-" + string(i)
			go apiLoadTester.runSubmission(queue, jobSetId, submission.Jobs)
		}
	}
}

func (apiLoadTester ArmadaApiLoadTester) runSubmission(queue string, jobSetId string, jobs []*domain.JobSubmissionDescription) {
	connection, err := createConnection(apiLoadTester.apiUrl)
	if err != nil {
		log.Errorf("Failed to connect to api because %s", err)
		return
	}
	defer connection.Close()

	client := api.NewSubmitClient(connection)
	for _, job := range jobs {
		jobRequest := createJobRequest(queue, jobSetId, job.Spec)
		for i := 0; i < job.Count; i++ {
			response, e := client.SubmitJob(timeout(), jobRequest)

			if e != nil {
				log.Error(e)
				break
			}
			log.Infof("Submitted job id: %s (set: %s)", response.JobId, jobSetId)
		}
	}
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

func createConnection(apiUrl string) (*grpc.ClientConn, error) {
	return grpc.Dial(apiUrl, grpc.WithInsecure())
}

//func createDefaultPodSpec() *v1.PodSpec {
//	cpuResource, _ := resource.ParseQuantity("50m")
//	memoryResource, _ := resource.ParseQuantity("64Mi")
//
//	resourceMap := map[v1.ResourceName]resource.Quantity{
//		v1.ResourceCPU:    cpuResource,
//		v1.ResourceMemory: memoryResource,
//	}
//
//
//	pod := v1.Pod{
//		Spec: v1.PodSpec{
//			Containers: []v1.Container{
//				v1.Container{
//					Name:"Container",
//
//					Image: "alpine:"
//
//					Resources: v1.ResourceRequirements {
//						Limits: resourceMap,
//						Requests: resourceMap,
//					},
//				},
//			},
//		},
//	}
//
//	return &pod
//}
//
//
//podSpec:
//terminationGracePeriodSeconds: 0
//restartPolicy: Never
//containers:
//- name: sleep
//imagePullPolicy: IfNotPresent
//image: mcr.microsoft.com/dotnet/core/runtime:2.2
//args:
//- sleep
//- 20s
//resources:
