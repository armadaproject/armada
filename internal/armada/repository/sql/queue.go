package sql

import (
	"database/sql"

	"github.com/G-Research/armada/pkg/api"
)

type QueueRepository struct {
	db sql.DB
}

func NewQueueRepository(db sql.DB) *QueueRepository {
	return &QueueRepository{db: db}
}

func (q QueueRepository) GetAllQueues() ([]*api.Queue, error) {
	panic("implement me")
}

func (q QueueRepository) GetQueue(name string) (*api.Queue, error) {
	panic("implement me")
}

func (q QueueRepository) CreateQueue(queue *api.Queue) error {
	panic("implement me")
}
