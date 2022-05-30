package service

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	v1 "k8s.io/api/core/v1"

	"github.com/G-Research/armada/internal/common"
	commonUtil "github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/etcdhealthmonitor"
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
	clusterContext                context2.ClusterContext
	queueClient                   api.AggregatedQueueClient
	minimumJobSize                common.ComputeResources
	avoidNodeLabelsOnRetry        []string
	EtcdHealthMonitor             *etcdhealthmonitor.EtcdHealthMonitor
	EtcdMaxFractionOfStorageInUse float64
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

	// If a health monitor is provided, avoid leasing jobs when etcd is almost full.
	if jobLeaseService.EtcdHealthMonitor != nil {
		fractionOfStorageInUse, err := jobLeaseService.EtcdHealthMonitor.MaxFractionOfStorageInUse()
		if err != nil {
			return nil, err
		}
		if fractionOfStorageInUse > jobLeaseService.EtcdMaxFractionOfStorageInUse {
			err := errors.Errorf("etcd is %f percent full, but the limit is %f percent", fractionOfStorageInUse, jobLeaseService.EtcdMaxFractionOfStorageInUse)
			err = errors.WithMessage(err, "did not request leases")
			return nil, errors.WithStack(err)
		}
	}

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
	// Send jobs on ch to another goroutine responsible for sending back acks.
	ch := make(chan *api.Job)
	g, _ := errgroup.WithContext(ctx)
	g.Go(func() error {
		// Close the chanel when all jobs have been received.
		// This ensures the goroutine responsible for sending acks exits.
		defer close(ch)
		for {
			res, err := stream.Recv()
			if err == io.EOF {
				return nil
			} else if err != nil {
				return err
			}
			ch <- res.Job
		}
	})

	// Get received jobs on the channel and send back acks.
	jobs := make([]*api.Job, 0)
	for {
		job := <-ch
		if job == nil {
			break // Channel closed.
		}

		// Send ack back to the server.
		err := stream.Send(&api.StreamingLeaseRequest{
			SubmittedJobs: []string{job.Id},
		})
		if err == io.EOF {
			break
		} else if err != nil {
			log.WithError(err).Error("error sending leases to server")
			break
		}
		jobs = append(jobs, job)
	}

	// Wait for receiver to exit.
	err = g.Wait()
	if err != nil {
		log.WithError(err).Error("error receiving leases from server")
	}

	return jobs, nil
}

func (jobLeaseService *JobLeaseService) ReturnLease(pod *v1.Pod) error {
	jobId := util.ExtractJobId(pod)
	ctx, cancel := common.ContextWithDefaultTimeout()
	defer cancel()

	avoidNodeLabels, err := getAvoidNodeLabels(pod, jobLeaseService.avoidNodeLabelsOnRetry, jobLeaseService.clusterContext)
	if err != nil {
		log.Warnf("Failed to get node labels to avoid on rerun for pod %s in namespace %s: %v", pod.Name, pod.Namespace, err)
		avoidNodeLabels = emptyOrderedStringMap()
	}

	log.Infof("Returning lease for job %s (will try to avoid these node labels next time: %v)", jobId, avoidNodeLabels)
	_, err = jobLeaseService.queueClient.ReturnLease(ctx, &api.ReturnLeaseRequest{ClusterId: jobLeaseService.clusterContext.GetClusterId(), JobId: jobId, AvoidNodeLabels: avoidNodeLabels})
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
