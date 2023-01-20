package database

type QueueRepository interface {
	GetAllQueues() ([]*Queue, error)
}
