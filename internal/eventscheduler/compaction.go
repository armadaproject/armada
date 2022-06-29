package scheduler

import (
	"github.com/pkg/errors"
)

// Log compacter.
type Compacter interface {
	// Push adds a key-value pair to the compacter.
	// The first argument is the key and the second the value.
	Push(interface{}, interface{}) error
	// Flush atomically moves all values stored by the compacter into a slice, which is returned,
	// and empties the internal storage of the compacter.
	Flush() ([]interface{}, error)
}

type QueuesCompacter struct {
	items map[string]*Queue
}

func (c *QueuesCompacter) Push(key interface{}, value interface{}) error {
	name, ok := key.(string)
	if !ok {
		return errors.Errorf("expected key to be of type string, but got %v", key)
	}
	queue, ok := value.(*Queue)
	if !ok {
		return errors.Errorf("expected value to be of type *Queue, but got %v", value)
	}
	if name != queue.Name {
		return errors.Errorf("expected queue.Name to be %s, but got %s", name, queue.Name)
	}
	if cached, ok := c.items[name]; ok {
		cached.Weight = queue.Weight
		cached.Deleted = queue.Deleted
	} else {
		c.items[name] = queue
	}
	return nil
}

func (c *QueuesCompacter) Flush() ([]interface{}, error) {
	// TODO: Use an atomic swap.
	items := c.items
	c.items = make(map[string]*Queue)
	rv := make([]interface{}, len(items), len(items))
	i := 0
	for _, item := range items {
		rv[i] = item
		i++
	}
	return rv, nil
}

type JobsCompacter struct {
	items map[string]*Job
}

func (c *JobsCompacter) Push(key interface{}, value interface{}) error {
	id, ok := key.(string)
	if !ok {
		return errors.Errorf("expected key to be of type string, but got %v", key)
	}
	job, ok := value.(*Job)
	if !ok {
		return errors.Errorf("expected value to be of type *Job, but got %v", value)
	}
	if id != job.Id {
		return errors.Errorf("expected job.Id to be %s, but got %s", id, job.Id)
	}
	if cached, ok := c.items[id]; ok {
		cached.Fragile = job.Fragile
		cached.Claims = job.Claims
		cached.priority = job.priority
	} else {
		c.items[id] = job
	}
	return nil
}

func (c *JobsCompacter) Flush() ([]interface{}, error) {
	// TODO: Use an atomic swap.
	items := c.items
	c.items = make(map[string]*Job)
	rv := make([]interface{}, len(items), len(items))
	i := 0
	for _, item := range items {
		rv[i] = item
		i++
	}
	return rv, nil
}

type LeaseCompacter struct {
	items map[string]*Lease
}

func (c *LeaseCompacter) Push(key interface{}, value interface{}) error {
	id, ok := key.(string)
	if !ok {
		return errors.Errorf("expected key to be of type string, but got %v", key)
	}
	lease, ok := value.(*Lease)
	if !ok {
		return errors.Errorf("expected value to be of type *Lease, but got %v", value)
	}
	if id != lease.Id {
		return errors.Errorf("expected job.Id to be %s, but got %s", id, lease.Id)
	}
	if cached, ok := c.items[id]; ok {
		cached.JobId = lease.JobId
		cached.ExecutorId = lease.ExecutorId
	} else {
		c.items[id] = lease
	}
	return nil
}

func (c *LeaseCompacter) Flush() ([]interface{}, error) {
	// TODO: Use an atomic swap.
	items := c.items
	c.items = make(map[string]*Lease)
	rv := make([]interface{}, len(items), len(items))
	i := 0
	for _, item := range items {
		rv[i] = item
		i++
	}
	return rv, nil
}
