package service

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	v1 "k8s.io/api/core/v1"

	"github.com/G-Research/armada/internal/common"
	commonUtil "github.com/G-Research/armada/internal/common/util"
	context2 "github.com/G-Research/armada/internal/executor/context"
	"github.com/G-Research/armada/internal/executor/job"
	"github.com/G-Research/armada/internal/executor/util"
	"github.com/G-Research/armada/pkg/api"
)

const maxPodRequestSize = 10000

type LeaseService interface {
	ReturnLease(pod *v1.Pod) error
	RequestJobLeases(availableResource *common.ComputeResources, nodes []api.NodeInfo, leasedResourceByQueue map[string]common.ComputeResources) ([]*api.Job, error)
	RenewJobLeases(jobs []*job.RunningJob) ([]*job.RunningJob, error)
	ReportDone(jobIds []string) error
}

type JobLeaseService struct {
	clusterContext         context2.ClusterContext
	queueClient            api.AggregatedQueueClient
	minimumJobSize         common.ComputeResources
	avoidNodeLabelsOnRetry []string
}

func NewJobLeaseService(
	clusterContext context2.ClusterContext,
	queueClient api.AggregatedQueueClient,
	minimumJobSize common.ComputeResources,
	avoidNodeLabelsOnRetry []string) *JobLeaseService {

	return &JobLeaseService{
		clusterContext:         clusterContext,
		queueClient:            queueClient,
		minimumJobSize:         minimumJobSize,
		avoidNodeLabelsOnRetry: avoidNodeLabelsOnRetry,
	}
}

func (jobLeaseService *JobLeaseService) RequestJobLeases(availableResource *common.ComputeResources, nodes []api.NodeInfo, leasedResourceByQueue map[string]common.ComputeResources) ([]*api.Job, error) {
	leasedQueueReports := make([]*api.QueueLeasedReport, 0, len(leasedResourceByQueue))
	for queueName, leasedResource := range leasedResourceByQueue {
		leasedQueueReport := &api.QueueLeasedReport{
			Name:            queueName,
			ResourcesLeased: leasedResource,
		}
		leasedQueueReports = append(leasedQueueReports, leasedQueueReport)
	}
	clusterLeasedReport := api.ClusterLeasedReport{
		ClusterId:  jobLeaseService.clusterContext.GetClusterId(),
		ReportTime: time.Now(),
		Queues:     leasedQueueReports,
	}

	// Setup a bidirectional gRPC stream.
	// The server sends jobs over this stream.
	// The executor sends back acks to indicate which jobs were successfully received.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := jobLeaseService.queueClient.StreamingLeaseJobs(ctx, grpc_retry.Disable())
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// The first message sent over the stream includes all information necessary
	// for the server to choose jobs to lease.
	// Subsequent messages only include ids of received jobs.
	err = stream.Send(&api.StreamingLeaseRequest{
		ClusterId:           jobLeaseService.clusterContext.GetClusterId(),
		Pool:                jobLeaseService.clusterContext.GetClusterPool(),
		Resources:           *availableResource,
		ClusterLeasedReport: clusterLeasedReport,
		Nodes:               nodes,
		MinimumJobSize:      jobLeaseService.minimumJobSize,
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// Goroutine receiving jobs from the server.
	// Also recevies ack confirmations from the server.
	// Send leases on ch to another goroutine responsible for sending back acks.
	// Give the channel a small buffer to allow for some asynchronicity.
	var numServerAcks uint32
	var numJobs uint32
	jobs := make([]*api.Job, 0)
	ch := make(chan *api.StreamingJobLease, 10)
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {

		// Close channel to ensure sending goroutine exits.
		defer close(ch)

		// Exit when until all acks have been confirmed.
		for numServerAcks == 0 || numServerAcks < numJobs {
			select {
			case <-ctx.Done():
				if ctx.Err() == context.DeadlineExceeded {
					return nil
				} else {
					return ctx.Err()
				}
			default:
				res, err := stream.Recv()
				if err == io.EOF {
					return nil
				} else if err != nil {
					return err
				}
				numJobs = res.GetNumJobs()
				numServerAcks = res.GetNumAcked()
				if res.Job != nil {
					jobs = append(jobs, res.Job)
				}
				ch <- res
			}
		}
		return nil
	})

	// Get received jobs on the channel and send back acks.
	g.Go(func() error {
		defer stream.CloseSend()
		for {
			select {
			case <-ctx.Done():
				if ctx.Err() == context.DeadlineExceeded {
					return nil
				} else {
					return ctx.Err()
				}
			case res := <-ch:
				if res == nil {
					return nil // Channel closed.
				}

				// Send ack back to the server.
				if res.Job != nil {
					err := stream.Send(&api.StreamingLeaseRequest{
						ReceivedJobIds: []string{res.Job.Id},
					})
					if err == io.EOF {
						return nil
					} else if err != nil {
						return err
					}
				}
			}
		}
	})

	// Wait for receiver to exit.
	err = g.Wait()
	if err != nil {
		log.WithError(err).Error("error receiving leases from server")
	}

	// If we received confirmation on the ack, we know the server is aware we received the job.
	// For the remaining jobs, return any leases.
	receivedJobs := jobs[:numServerAcks]

	// Expire jobs the server never confirmed the ack of.
	jobsToReturn := jobs[numServerAcks:]
	var result *multierror.Error
	for _, job := range jobsToReturn {
		err := jobLeaseService.ReturnLeaseById(job.Id, nil)
		result = multierror.Append(result, err)
	}
	err = result.ErrorOrNil()
	if err != nil {
		log.WithError(err).Error("error returning leases to server")
	}

	return receivedJobs, nil
}

func (jobLeaseService *JobLeaseService) ReturnLease(pod *v1.Pod) error {
	jobId := util.ExtractJobId(pod)
	avoidNodeLabels, err := getAvoidNodeLabels(pod, jobLeaseService.avoidNodeLabelsOnRetry, jobLeaseService.clusterContext)
	if err != nil {
		log.Warnf("Failed to get node labels to avoid on rerun for pod %s in namespace %s: %v", pod.Name, pod.Namespace, err)
		avoidNodeLabels = emptyOrderedStringMap()
	}
	return jobLeaseService.ReturnLeaseById(jobId, avoidNodeLabels)
}

func (jobLeaseService *JobLeaseService) ReturnLeaseById(jobId string, nodeLabelsToAvoid *api.OrderedStringMap) error {
	ctx, cancel := common.ContextWithDefaultTimeout()
	defer cancel()

	if nodeLabelsToAvoid != nil && len(nodeLabelsToAvoid.Entries) > 0 {
		log.Infof("Returning lease for job %s (will try to avoid these node labels next time: %v)", jobId, nodeLabelsToAvoid)
	} else {
		log.Infof("Returning lease for job %s", jobId)
	}
	_, err := jobLeaseService.queueClient.ReturnLease(ctx, &api.ReturnLeaseRequest{ClusterId: jobLeaseService.clusterContext.GetClusterId(), JobId: jobId, AvoidNodeLabels: nodeLabelsToAvoid})
	return err
}

func (jobLeaseService *JobLeaseService) ReportDone(jobIds []string) error {
	if len(jobIds) <= 0 {
		return nil
	}
	ctx, cancel := common.ContextWithDefaultTimeout()
	defer cancel()
	log.Infof("Reporting done for jobs %s", strings.Join(jobIds, ","))
	_, err := jobLeaseService.queueClient.ReportDone(ctx, &api.IdList{Ids: jobIds})

	return err
}

func (jobLeaseService *JobLeaseService) RenewJobLeases(jobs []*job.RunningJob) ([]*job.RunningJob, error) {
	if len(jobs) <= 0 {
		return []*job.RunningJob{}, nil
	}
	jobIds := extractJobIds(jobs)
	log.Infof("Renewing lease for %s", strings.Join(jobIds, ","))

	ctx, cancel := common.ContextWithDefaultTimeout()
	defer cancel()
	renewedJobIds, err := jobLeaseService.queueClient.RenewLease(ctx,
		&api.RenewLeaseRequest{
			ClusterId: jobLeaseService.clusterContext.GetClusterId(),
			Ids:       jobIds})
	if err != nil {
		log.Errorf("Failed to renew lease for jobs because %s", err)
		return nil, err
	}

	failedIds := commonUtil.SubtractStringList(jobIds, renewedJobIds.Ids)
	failedJobs := filterRunningJobsByIds(jobs, failedIds)

	if len(failedIds) > 0 {
		log.Warnf("Server has prevented renewing of job lease for jobs %s", strings.Join(failedIds, ","))
	}

	return failedJobs, nil
}

func getAvoidNodeLabels(pod *v1.Pod, avoidNodeLabelsOnRetry []string, clusterContext context2.ClusterContext) (*api.OrderedStringMap, error) {
	if len(avoidNodeLabelsOnRetry) == 0 {
		return emptyOrderedStringMap(), nil
	}

	nodeName := pod.Spec.NodeName
	if nodeName == "" {
		log.Infof("Pod %s in namespace %s doesn't have nodeName set, so returning empty avoid_node_labels", pod.Name, pod.Namespace)
		return emptyOrderedStringMap(), nil
	}

	node, err := clusterContext.GetNode(nodeName)
	if err != nil {
		return nil, fmt.Errorf("Could not get node %s from Kubernetes api: %v", nodeName, err)
	}

	result := api.OrderedStringMap{}
	for _, label := range avoidNodeLabelsOnRetry {
		val, exists := node.Labels[label]
		if exists {
			result.Entries = append(result.Entries, &api.StringKeyValuePair{Key: label, Value: val})
		}
	}

	if len(result.Entries) == 0 {
		return nil, fmt.Errorf("None of the labels specified in avoidNodeLabelsOnRetry (%s) were found on node %s", strings.Join(avoidNodeLabelsOnRetry, ", "), nodeName)
	}
	return &result, nil
}

func emptyOrderedStringMap() *api.OrderedStringMap {
	return &api.OrderedStringMap{Entries: []*api.StringKeyValuePair{}}
}
