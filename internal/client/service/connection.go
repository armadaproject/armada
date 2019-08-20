package service

import (
	"context"
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/client/domain"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	v1 "k8s.io/api/core/v1"
	"time"
)

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

func WithConnection(apiConnectionDetails *domain.ArmadaApiConnectionDetails, action func(*grpc.ClientConn)) {
	conn, err := createConnection(apiConnectionDetails)

	if err != nil {
		log.Errorf("Failed to connect to api because %s", err)
	}
	defer conn.Close()

	action(conn)
}

func createConnection(connectionDetails *domain.ArmadaApiConnectionDetails) (*grpc.ClientConn, error) {
	creds := connectionDetails.Credentials
	if creds.Username == "" || creds.Password == "" {
		return grpc.Dial(connectionDetails.Url, grpc.WithInsecure())
	} else {
		return grpc.Dial(
			connectionDetails.Url,
			grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(nil, "")),
			grpc.WithPerRPCCredentials(&creds))
	}
}
