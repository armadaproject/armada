package sql

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/G-Research/armada/pkg/api"
)

type QueueRepository struct {
	db *sql.DB
}

func NewQueueRepository(db *sql.DB) *QueueRepository {
	return &QueueRepository{db: db}
}

func (q QueueRepository) GetAllQueues() ([]*api.Queue, error) {
	rows, err := q.db.Query("SELECT queue FROM queue")
	if err != nil {
		return nil, err
	}
	return readQueues(rows)
}

func (q QueueRepository) GetQueue(name string) (*api.Queue, error) {
	rows, err := q.db.Query("SELECT queue FROM queue WHERE name = $1", name)
	if err != nil {
		return nil, err
	}
	queues, err := readQueues(rows)
	if err != nil {
		return nil, err
	}
	if len(queues) < 1 {
		return nil, fmt.Errorf("queue %v not found", name)
	}
	return queues[0], err
}

func (q QueueRepository) CreateQueue(queue *api.Queue) error {
	data, err := json.Marshal(queue)
	if err != nil {
		return err
	}
	_, err = upsert(q.db, "queue", "name", []string{"queue"}, []interface{}{queue.Name, data})
	return err
}

func readQueues(rows *sql.Rows) ([]*api.Queue, error) {
	queues := []*api.Queue{}
	err := readByteRows(rows, func(b []byte) error {
		var q *api.Queue
		err := json.Unmarshal(b, &q)
		queues = append(queues, q)
		return err
	})
	return queues, err
}
