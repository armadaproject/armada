package introspection

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/pkg/api"
	introspectionapi "github.com/armadaproject/armada/pkg/api/introspection"
)

type IntrospectionServer struct {
	*introspectionapi.UnimplementedIntrospectionServer
	kube       KubeClientFactory
	runs       JobRunDetailsGetter
	jobs       JobDetailsGetter
	podLister  corev1listers.PodLister
	nodeLister corev1listers.NodeLister
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

func (s *IntrospectionServer) WithListers(pods corev1listers.PodLister, nodes corev1listers.NodeLister) *IntrospectionServer {
	s.podLister = pods
	s.nodeLister = nodes
	return s
}

func (s *IntrospectionServer) KubeDescribeNode(ctx context.Context, req *introspectionapi.DescribeNodeRequest) (*introspectionapi.DescribeNodeResponse, error) {
	if req.GetCluster() == "" {
		return nil, status.Error(codes.InvalidArgument, "cluster is required")
	}
	if req.GetNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}
	if s.nodeLister == nil {
		return nil, status.Error(codes.FailedPrecondition, "node lister not configured")
	}

	node, err := s.nodeLister.Get(req.NodeId)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, status.Errorf(codes.NotFound, "node %q not found", req.NodeId)
		}
		return nil, status.Errorf(codes.Unavailable, "failed to get node %q: %v", req.NodeId, err)
	}

	resp := &introspectionapi.DescribeNodeResponse{
		Cluster:  req.Cluster,
		NodeName: node.Name,
		NodeUid:  string(node.UID),
		Labels:   node.Labels,
	}

	for _, a := range node.Status.Addresses {
		resp.Addresses = append(resp.Addresses, &introspectionapi.NodeAddress{
			Type:    string(a.Type),
			Address: a.Address,
		})
	}

	for _, t := range node.Spec.Taints {
		resp.Taints = append(resp.Taints, &introspectionapi.Taint{
			Key:    t.Key,
			Value:  t.Value,
			Effect: string(t.Effect),
		})
	}

	for _, c := range node.Status.Conditions {
		resp.Conditions = append(resp.Conditions, &introspectionapi.NodeCondition{
			Type:               string(c.Type),
			Status:             string(c.Status),
			Reason:             c.Reason,
			Message:            c.Message,
			LastTransitionTime: c.LastTransitionTime.String(),
		})
	}

	resp.Capacity = &introspectionapi.ResourceList{Resources: map[string]string{}}
	for k, v := range node.Status.Capacity {
		resp.Capacity.Resources[string(k)] = v.String()
	}
	resp.Allocatable = &introspectionapi.ResourceList{Resources: map[string]string{}}
	for k, v := range node.Status.Allocatable {
		resp.Allocatable.Resources[string(k)] = v.String()
	}

	if req.IncludeRaw {
		b, err := json.Marshal(node)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to marshal node json: %v", err)
		}
		resp.RawNodeJson = b
	}

	return resp, nil
}

func (s *IntrospectionServer) KubeDescribeNodeByJobRun(ctx context.Context, req *introspectionapi.DescribeNodeByJobRunRequest) (*introspectionapi.DescribeNodeResponse, error) {
	if req.GetRunId() == "" {
		return nil, status.Error(codes.InvalidArgument, "run_id is required")
	}
	if s.runs == nil {
		return nil, status.Error(codes.FailedPrecondition, "run details getter is not configured")
	}

	resp, err := s.runs.GetJobRunDetails(ctx, &api.JobRunDetailsRequest{RunIds: []string{req.RunId}})
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

	return s.KubeDescribeNode(ctx, &introspectionapi.DescribeNodeRequest{
		Cluster:    d.Cluster,
		NodeId:     d.Node,
		IncludeRaw: req.IncludeRaw,
	})
}

func (s *IntrospectionServer) KubeDescribeNodeByJobId(ctx context.Context, req *introspectionapi.DescribeNodeByJobIdRequest) (*introspectionapi.DescribeNodeResponse, error) {
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
		JobIds:       []string{req.JobId},
		ExpandJobRun: false,
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
		return nil, status.Errorf(codes.FailedPrecondition, "job_id %q has no run yet (not leased/scheduled)", req.JobId)
	}

	return s.KubeDescribeNodeByJobRun(ctx, &introspectionapi.DescribeNodeByJobRunRequest{
		RunId:      runId,
		IncludeRaw: req.GetIncludeRaw(),
	})
}

func (s *IntrospectionServer) KubeDescribeJobPod(ctx context.Context, req *introspectionapi.DescribeJobPodRequest) (*introspectionapi.DescribeJobPodResponse, error) {
	if req.GetJobId() == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	if s.jobs == nil {
		return nil, status.Error(codes.FailedPrecondition, "job details getter is not configured")
	}
	if s.runs == nil {
		return nil, status.Error(codes.FailedPrecondition, "run details getter is not configured")
	}
	if s.podLister == nil {
		return nil, status.Error(codes.FailedPrecondition, "pod lister is not configured")
	}

	jobResp, err := s.jobs.GetJobDetails(ctx, &api.JobDetailsRequest{
		JobIds:       []string{req.JobId},
		ExpandJobRun: false,
	})
	if err != nil {
		return nil, err
	}
	jd := jobResp.JobDetails[req.JobId]
	if jd == nil {
		return nil, status.Errorf(codes.NotFound, "job_id %q not found", req.JobId)
	}
	if jd.Namespace == "" {
		return nil, status.Errorf(codes.FailedPrecondition, "job_id %q has no namespace recorded", req.JobId)
	}

	runId := req.GetRunId()
	if runId == "" {
		runId = jd.GetLatestRunId()
	}
	if runId == "" {
		return nil, status.Errorf(codes.FailedPrecondition, "job_id %q has no run yet", req.JobId)
	}

	cluster := req.GetCluster()
	if cluster == "" {
		runResp, err := s.runs.GetJobRunDetails(ctx, &api.JobRunDetailsRequest{RunIds: []string{runId}})
		if err != nil {
			return nil, err
		}
		rd := runResp.JobRunDetails[runId]
		if rd == nil {
			return nil, status.Errorf(codes.NotFound, "run_id %q not found", runId)
		}
		cluster = rd.Cluster
	}

	podName := common.PodName(req.JobId)
	pod, err := s.podLister.Pods(jd.Namespace).Get(podName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, status.Errorf(codes.NotFound, "pod %q in namespace %q not found (may have been garbage collected)", podName, jd.Namespace)
		}
		return nil, status.Errorf(codes.Unavailable, "failed to get pod %q: %v", podName, err)
	}

	resp := &introspectionapi.DescribeJobPodResponse{
		Cluster:      cluster,
		PodNamespace: jd.Namespace,
		PodName:      pod.Name,
		PodUid:       string(pod.UID),
		NodeName:     pod.Spec.NodeName,
		Phase:        string(pod.Status.Phase),
	}

	for _, cs := range pod.Status.ContainerStatuses {
		cStatus := &introspectionapi.ContainerStatus{
			Name:         cs.Name,
			Ready:        cs.Ready,
			RestartCount: cs.RestartCount,
		}
		switch {
		case cs.State.Running != nil:
			cStatus.State = "Running"
		case cs.State.Terminated != nil:
			cStatus.State = "Terminated"
			cStatus.Reason = cs.State.Terminated.Reason
			cStatus.Message = cs.State.Terminated.Message
		case cs.State.Waiting != nil:
			cStatus.State = "Waiting"
			cStatus.Reason = cs.State.Waiting.Reason
			cStatus.Message = cs.State.Waiting.Message
		}
		resp.Containers = append(resp.Containers, cStatus)
	}

	for _, c := range pod.Status.Conditions {
		resp.Conditions = append(resp.Conditions, &introspectionapi.PodCondition{
			Type:               string(c.Type),
			Status:             string(c.Status),
			Reason:             c.Reason,
			Message:            c.Message,
			LastTransitionTime: c.LastTransitionTime.String(),
		})
	}

	if req.IncludeEvents {
		if s.kube == nil {
			return nil, status.Error(codes.FailedPrecondition, "kube client factory is required for events")
		}
		kubeClient, err := s.kube.Client(cluster)
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "failed to get kube client for cluster %q: %v", cluster, err)
		}
		fieldSelector := fmt.Sprintf("involvedObject.name=%s,involvedObject.namespace=%s", podName, jd.Namespace)
		events, err := kubeClient.CoreV1().Events(jd.Namespace).List(ctx, metav1.ListOptions{FieldSelector: fieldSelector})
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "failed to list events for pod %q: %v", podName, err)
		}
		for _, e := range events.Items {
			var eventTime string
			if !e.EventTime.IsZero() {
				eventTime = e.EventTime.UTC().Format(time.RFC3339)
			}
			resp.Events = append(resp.Events, &introspectionapi.PodEvent{
				Type:           e.Type,
				Reason:         e.Reason,
				Message:        e.Message,
				FirstTimestamp: e.FirstTimestamp.String(),
				LastTimestamp:  e.LastTimestamp.String(),
				Count:          e.Count,
				EventTime:      eventTime,
			})
		}
	}

	if req.IncludeRaw {
		b, err := json.Marshal(pod)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to marshal pod json: %v", err)
		}
		resp.RawPodJson = b
	}

	return resp, nil
}
