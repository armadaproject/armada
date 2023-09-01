package scheduling

import (
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/armada/repository"
	"github.com/armadaproject/armada/internal/common/context"
	"github.com/armadaproject/armada/pkg/api"
)

type LeaseManager struct {
	jobRepository       repository.JobRepository
	queueRepository     repository.QueueRepository
	eventStore          repository.EventStore
	leaseExpiryDuration time.Duration
}

func NewLeaseManager(
	jobRepository repository.JobRepository,
	queueRepository repository.QueueRepository,
	eventStore repository.EventStore,
	leaseExpiryDuration time.Duration,
) *LeaseManager {
	return &LeaseManager{
		jobRepository:       jobRepository,
		queueRepository:     queueRepository,
		eventStore:          eventStore,
		leaseExpiryDuration: leaseExpiryDuration,
	}
}

func (l *LeaseManager) ExpireLeases() {
	queues, e := l.queueRepository.GetAllQueues()
	if e != nil {
		log.Error(e)
		return
	}

	deadline := time.Now().Add(-l.leaseExpiryDuration)
	for _, queue := range queues {
		jobs, e := l.jobRepository.ExpireLeases(queue.Name, deadline)
		now := time.Now()
		if e != nil {
			log.Error(e)
		} else {
			for _, job := range jobs {
				event, e := api.Wrap(&api.JobLeaseExpiredEvent{
					JobId:    job.Id,
					Queue:    job.Queue,
					JobSetId: job.JobSetId,
					Created:  now,
				})
				if e != nil {
					log.Error(e)
				} else {
					e := l.eventStore.ReportEvents(context.Background(), []*api.EventMessage{event})
					if e != nil {
						log.Error(e)
					}
				}
			}
		}
	}
}
