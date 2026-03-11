package introspection

import (
	"context"
	"encoding/json"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"github.com/armadaproject/armada/pkg/api"
	introspectionapi "github.com/armadaproject/armada/pkg/api/introspection"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type IntrospectionServer struct {
	*introspectionapi.UnimplementedIntrospectionServer
	kube KubeClientFactory
	runs JobRunDetailsGetter
	jobs JobDetailsGetter
}

type JobRunDetailsGetter interface {
	GetJobRunDetails(ctx context.Context, req *api.JobRunDetailsRequest) (*api.JobRunDetailsResponse, error)
}

type JobDetailsGetter interface {
	GetJobDetails(ctx context.Context, req *api.JobDetailsRequest) (*api.JobDetailsResponse, error)
}

func NewIntrospectionServer(
	kube KubeClientFactory, runs JobRunDetailsGetter, jobs JobDetailsGetter) *IntrospectionServer {
	return &IntrospectionServer{
		kube: kube,
		runs: runs,
		jobs: jobs,
	}
}

func (s *IntrospectionServer) DescribeNode (ctx context.Context, req *introspectionapi.DescribeNodeRequest) (*introspectionapi.DescribeNodeResponse, error) {
	if req.GetCluster() == "" {
		return nil, status.Error(codes.InvalidArgument, "cluster is required")
	}

	if req.GetNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}

	if s.kube == nil {
		return nil, status.Error(codes.FailedPrecondition, "kube client factory not configured")
	}

	client, err := s.kube.Client(req.Cluster)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, status.Errorf(codes.NotFound, "node %q not found", req.NodeId)
		}
		return nil, status.Errorf(codes.Unavailable, "failed to get node %q: %v", req.NodeId, err)
	}

	node, err:= client.CoreV1().Nodes().Get(ctx, req.NodeId, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, status.Errorf(codes.NotFound, "node %q not found", req.NodeId)
		}
		return nil, status.Errorf(codes.Unavailable, "failed to get node %q: %v", req.NodeId, err)
	}

	resp := &introspectionapi.DescribeNodeResponse{
		Cluster: req.Cluster,
		NodeName: node.Name,
		NodeUid: string(node.UID),
		Labels: node.Labels,
	}

	for _, a := range node.Status.Addresses {
		resp.Addresses = append(resp.Addresses, &introspectionapi.NodeAddress{
			Type: string(a.Type),
			Address: a.Address,
		})
	}

	for _, t := range node.Spec.Taints {
		resp.Taints = append(resp.Taints, &introspectionapi.Taint{
			Key: t.Key,
			Value: t.Value,
			Effect: string(t.Effect),
		})
	}

	for _, c:= range node.Status.Conditions {
		resp.Conditions = append(resp.Conditions, &introspectionapi.NodeCondition{
			Type: string(c.Type),
			Status: string(c.Status),
			Reason: c.Reason,
			Message: c.Message,
			LastTransitionTime: c.LastTransitionTime.String(),
		})
	}

	resp.Capacity = &introspectionapi.ResourceList{Resources: map[string]string{}}
	for k,v := range node.Status.Capacity {
		resp.Capacity.Resources[string(k)] = v.String()
	}
	resp.Allocatable = &introspectionapi.ResourceList{Resources: map[string]string{}}
	for k,v := range node.Status.Allocatable {
		resp.Allocatable.Resources[string(k)] = v.String()
	}

	if req.IncludeRaw {
		b, err := json.Marshal(node)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to marshal node josn: %v", err)
		}
		resp.RawNodeJson = b
	}

	return resp, nil
}

func (s *IntrospectionServer) DescribeNodeByJobRun(ctx context.Context, req *introspectionapi.DescribeNodeByJobRunRequest) (*introspectionapi.DescribeNodeResponse, error) {
	if req.GetRunId() == "" {
		return nil, status.Error(codes.InvalidArgument, "run_id is required")
	}
	if s.runs == nil {
		return nil, status.Error(codes.FailedPrecondition, "run details getter is not configured")
	}

	resp, err := s.runs.GetJobRunDetails(ctx, &api.JobRunDetailsRequest{RunIds: []string{req.RunId} })
	if err != nil {
		return nil, err
	}
	d := resp.JobRunDetails[req.RunId]
	if d == nil {
		return nil, status.Errorf(codes.NotFound, "run_id %q not found", req.RunId)
	}
	if d.Node == "" {
		return nil, status.Errorf(codes.FailedPrecondition, "run_id %q has no node assigned", req.RunId)
	}

	return s.DescribeNode(ctx, &introspectionapi.DescribeNodeRequest{
		Cluster: d.Cluster,
		NodeId: d.Node,
		IncludeRaw: req.IncludeRaw,
	})
}

func (s *IntrospectionServer) DescribeNodeByJobId(ctx context.Context, req *introspectionapi.DescribeNodeByJobIdRequest) (*introspectionapi.DescribeNodeResponse, error) {
	if req.GetJobId() == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	if s.jobs == nil {
		return nil, status.Error(codes.FailedPrecondition, "job details getter is not configured")
	}
	if s.runs == nil {
		return nil, status.Error(codes.FailedPrecondition, "run details getter is not configured")
	}

	jr, err := s.jobs.GetJobDetails(ctx, &api.JobDetailsRequest{
		JobIds: 		[]string{req.JobId},
		ExpandJobRun: 	false,
	})
	if err != nil {
		return nil, err
	}
	jd := jr.JobDetails[req.JobId]
	if jd == nil {
		return nil, status.Errorf(codes.NotFound, "job_id %q not found", req.JobId)
	}

	runId := jd.GetLatestRunId()
	if runId == "" {
		return nil, status.Errorf(codes.FailedPrecondition, "job_id %q has no run yet(not leased/scheduled)", req.JobId)
	}

	return s.DescribeNodeByJobRun(ctx, &introspectionapi.DescribeNodeByJobRunRequest{
		RunId: runId,
		IncludeRaw: req.GetIncludeRaw(),
	})
}

// func (s *IntrospectionServer) DescribeJobPod (ctx context.Context, req *introspectionapi.DescribeJobPodRequest) (*introspectionapi.DescribeJobPodResponse, error) {
// 	if req.GetJobId() == "" {
// 		return nil, status.Error(codes.InvalidArgument, "job_id is required")
// 	}
// 	if s.jobs == nil {
// 		return nil, status.Error(codes.FailedPrecondition, "job details getter is not configured")
// 	}
// 	if s.runs == nil {

// 	}
// }

