package podwriter

import (
	"context"
	"time"

	v1 "k8s.io/api/core/v1"
)

// Status is the outcome for a single pod in a batch.
type Status string

const (
	Created       Status = "Created"
	AlreadyExists Status = "AlreadyExists"
	Rejected      Status = "Rejected"
	Failed        Status = "Failed"
)

// Writer writes pods directly to etcd.
type Writer interface {
	CreateBatch(ctx context.Context, req BatchRequest) BatchResult
}

// BatchRequest is a single bulk write request.
type BatchRequest struct {
	User   string
	Groups []string
	Pods   []PodRequest
}

// PodRequest is one pod in a batch.
type PodRequest struct {
	Namespace      string
	IdempotencyKey string
	Pod            *v1.Pod
}

// PodResult is the outcome for one pod.
type PodResult struct {
	Namespace string
	Name      string
	Status    Status
	Reason    string
	Transient bool
	Pod       *v1.Pod
}

// BatchResult is the outcome for a batch.
type BatchResult struct {
	Results []PodResult
}

// Authorizer checks whether a user may create pods in a namespace.
// The POC passes nil; the executor integration supplies a cached
// SubjectAccessReview implementation.
type Authorizer interface {
	Authorize(ctx context.Context, user string, groups []string, namespace string) error
}

// Config controls batching and the etcd key prefix.
type Config struct {
	Prefix          string
	MaxTxnOps       int
	MaxRequestBytes int
	RequestTimeout  time.Duration
	// Concurrency is how many etcd transactions of one batch are committed
	// in parallel. Chunks hold disjoint keys, so this cannot interleave
	// writes to the same pod.
	Concurrency int
}

func (c Config) withDefaults() Config {
	if c.Prefix == "" {
		c.Prefix = "/registry"
	}
	if c.MaxTxnOps <= 0 {
		c.MaxTxnOps = 128
	}
	if c.MaxRequestBytes <= 0 {
		c.MaxRequestBytes = 1572864
	}
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = 30 * time.Second
	}
	if c.Concurrency <= 0 {
		c.Concurrency = 1
	}
	return c
}

// KeyValue is one etcd put.
type KeyValue struct {
	Key   string
	Value []byte
}

// KV is the small etcd surface the writer needs. The production
// implementation uses a transaction; tests use an in-memory map.
type KV interface {
	// CreateBatch creates all keys atomically. It returns false when at
	// least one key already exists, in which case no keys were written.
	CreateBatch(ctx context.Context, kvs []KeyValue) (bool, error)
	Get(ctx context.Context, key string) ([]byte, error)
}
