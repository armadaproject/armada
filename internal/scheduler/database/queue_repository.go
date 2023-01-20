package database

import (
	legacyrepository "github.com/armadaproject/armada/internal/armada/repository"
	"github.com/go-redis/redis"
)

// QueueRepository is an interface to be implemented by structs which provide queue information
type QueueRepository interface {
	GetAllQueues() ([]*Queue, error)
}

// LegacyQueueRepository is a QueueRepository which is backed by Armada's redis store
type LegacyQueueRepository struct {
	backingRepo legacyrepository.QueueRepository
}

func NewLegacyQueueRepository(db redis.UniversalClient) *LegacyQueueRepository {
	return &LegacyQueueRepository{
		backingRepo: legacyrepository.NewRedisQueueRepository(db),
	}
}

func (r *LegacyQueueRepository) GetAllQueues() ([]*Queue, error) {
	legacyQueues, err := r.backingRepo.GetAllQueues()
	if err != nil {
		return nil, err
	}
	queues := make([]*Queue, len(legacyQueues))
	for i, legacyQueue := range legacyQueues {
		queues[i] = &Queue{
			Name:   legacyQueue.Name,
			Weight: float64(legacyQueue.PriorityFactor),
		}
	}
	return queues, nil
}
