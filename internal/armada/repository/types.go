package repository

import (
	"time"

	"github.com/G-Research/armada/internal/armada/repository/redis"
	"github.com/G-Research/armada/pkg/api"
)

type JobQueueRepository interface {
	PeekQueue(queue string, limit int64) ([]*api.Job, error)
	TryLeaseJobs(clusterId string, queue string, jobs []*api.Job) ([]*api.Job, error)
}

type JobRepository interface {
	JobQueueRepository
	AddJobs(job []*api.Job) ([]*redis.SubmitJobResult, error)
	GetExistingJobsByIds(ids []string) ([]*api.Job, error)
	FilterActiveQueues(queues []*api.Queue) ([]*api.Queue, error)
	GetQueueSizes(queues []*api.Queue) (sizes []int64, e error)
	RenewLease(clusterId string, jobIds []string) (renewed []string, e error)
	ExpireLeases(queue string, deadline time.Time) (expired []*api.Job, e error)
	ReturnLease(clusterId string, jobId string) (returnedJob *api.Job, err error)
	DeleteJobs(jobs []*api.Job) map[*api.Job]error
	GetActiveJobIds(queue string, jobSetId string) ([]string, error)
	GetQueueActiveJobSets(queue string) ([]*api.JobSetInfo, error)
}

type QueueRepository interface {
	GetAllQueues() ([]*api.Queue, error)
	GetQueue(name string) (*api.Queue, error)
	CreateQueue(queue *api.Queue) error
}

type UsageRepository interface {
	GetClusterUsageReports() (map[string]*api.ClusterUsageReport, error)
	GetClusterPriority(clusterId string) (map[string]float64, error)
	GetClusterPriorities(clusterIds []string) (map[string]map[string]float64, error)
	GetClusterLeasedReports() (map[string]*api.ClusterLeasedReport, error)

	UpdateCluster(report *api.ClusterUsageReport, priorities map[string]float64) error
	UpdateClusterLeased(report *api.ClusterLeasedReport) error
}

type EventStore interface {
	ReportEvents(message []*api.EventMessage) error
}

type EventRepository interface {
	ReadEvents(queue, jobSetId string, lastId string, limit int64, block time.Duration) ([]*api.EventStreamMessage, error)
	GetLastMessageId(queue, jobSetId string) (string, error)
}

type SchedulingInfoRepository interface {
	GetClusterSchedulingInfo() (map[string]*api.ClusterSchedulingInfoReport, error)
	UpdateClusterSchedulingInfo(report *api.ClusterSchedulingInfoReport) error
}
