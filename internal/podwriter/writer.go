package podwriter

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/executor/domain"
)

// EtcdWriter implements Writer using the shared pod preparation and encoding
// logic.
type EtcdWriter struct {
	kv         KV
	encoder    *Encoder
	config     Config
	prepare    PrepareOptions
	authorizer Authorizer
}

func NewEtcdWriter(kv KV, config Config, prepare PrepareOptions, authorizer Authorizer) *EtcdWriter {
	return &EtcdWriter{
		kv:         kv,
		encoder:    NewEncoder(),
		config:     config.withDefaults(),
		prepare:    prepare,
		authorizer: authorizer,
	}
}

type preparedPod struct {
	index          int
	namespace      string
	name           string
	idempotencyKey string
	pod            *v1.Pod
	key            string
	value          []byte
}

func (w *EtcdWriter) CreateBatch(ctx context.Context, req BatchRequest) BatchResult {
	if w.config.RequestTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, w.config.RequestTimeout)
		defer cancel()
	}

	results := make([]PodResult, len(req.Pods))
	prepared := make([]preparedPod, 0, len(req.Pods))
	authCache := map[string]error{}

	for i, request := range req.Pods {
		pod, rejected := w.prepareRequest(ctx, i, req.User, req.Groups, request, authCache)
		if rejected != nil {
			results[i] = *rejected
			continue
		}
		prepared = append(prepared, *pod)
	}

	w.commitChunks(ctx, w.chunk(prepared), results)
	return BatchResult{Results: results}
}

// commitChunks writes chunks of prepared pods in parallel. Chunks hold
// disjoint keys, so parallel commits cannot write the same pod twice.
func (w *EtcdWriter) commitChunks(ctx context.Context, chunks [][]preparedPod, results []PodResult) {
	if len(chunks) == 0 {
		return
	}
	if w.config.Concurrency <= 1 {
		for _, chunk := range chunks {
			if err := ctx.Err(); err != nil {
				for _, pod := range chunk {
					results[pod.index] = failedResult(pod, err, true)
				}
				continue
			}
			w.storeChunkResults(ctx, chunk, results)
		}
		return
	}
	var waitGroup sync.WaitGroup
	semaphore := make(chan struct{}, w.config.Concurrency)
	for _, chunk := range chunks {
		if err := ctx.Err(); err != nil {
			for _, pod := range chunk {
				results[pod.index] = failedResult(pod, err, true)
			}
			continue
		}
		waitGroup.Add(1)
		semaphore <- struct{}{}
		go func(chunk []preparedPod) {
			defer waitGroup.Done()
			defer func() { <-semaphore }()
			w.storeChunkResults(ctx, chunk, results)
		}(chunk)
	}
	waitGroup.Wait()
}

func (w *EtcdWriter) storeChunkResults(ctx context.Context, chunk []preparedPod, results []PodResult) {
	chunkResults := w.createChunk(ctx, chunk, 0)
	for i := range chunk {
		results[chunk[i].index] = chunkResults[i]
	}
}

func (w *EtcdWriter) prepareRequest(
	ctx context.Context,
	index int,
	user string,
	groups []string,
	request PodRequest,
	authCache map[string]error,
) (*preparedPod, *PodResult) {
	if request.Pod == nil {
		return nil, &PodResult{
			Namespace: request.Namespace,
			Status:    Rejected,
			Reason:    "pod is nil",
		}
	}
	namespace := request.Namespace
	if namespace == "" {
		namespace = request.Pod.Namespace
	}
	if namespace == "" {
		return nil, &PodResult{
			Name:   request.Pod.Name,
			Status: Rejected,
			Reason: "namespace is empty",
		}
	}
	if request.Pod.Namespace != "" && request.Pod.Namespace != namespace {
		return nil, &PodResult{
			Namespace: namespace,
			Name:      request.Pod.Name,
			Status:    Rejected,
			Reason:    "request namespace does not match pod namespace",
		}
	}
	if w.authorizer != nil {
		authErr, cached := authCache[namespace]
		if !cached {
			authErr = w.authorizer.Authorize(ctx, user, groups, namespace)
			authCache[namespace] = authErr
		}
		if authErr != nil {
			return nil, &PodResult{
				Namespace: namespace,
				Name:      request.Pod.Name,
				Status:    Rejected,
				Reason:    "not authorized: " + authErr.Error(),
			}
		}
	}

	pod := request.Pod.DeepCopy()
	pod.Namespace = namespace
	if err := Prepare(pod, w.prepare); err != nil {
		return nil, &PodResult{
			Namespace: namespace,
			Name:      pod.Name,
			Status:    Rejected,
			Reason:    err.Error(),
		}
	}
	if err := Validate(pod); err != nil {
		return nil, &PodResult{
			Namespace: namespace,
			Name:      pod.Name,
			Status:    Rejected,
			Reason:    err.Error(),
		}
	}

	key := EncodeKey(w.config.Prefix, pod.Namespace, pod.Name)
	value, err := w.encoder.EncodePod(pod)
	if err != nil {
		return nil, &PodResult{
			Namespace: namespace,
			Name:      pod.Name,
			Status:    Rejected,
			Reason:    err.Error(),
		}
	}
	if len(key)+len(value) > w.config.MaxRequestBytes {
		return nil, &PodResult{
			Namespace: namespace,
			Name:      pod.Name,
			Status:    Rejected,
			Reason:    "pod exceeds the max etcd request size",
		}
	}
	return &preparedPod{
		index:          index,
		namespace:      namespace,
		name:           pod.Name,
		idempotencyKey: request.IdempotencyKey,
		pod:            pod,
		key:            key,
		value:          value,
	}, nil
}

func (w *EtcdWriter) chunk(pods []preparedPod) [][]preparedPod {
	maxOps := w.config.MaxTxnOps / 2 // one compare + one put per pod
	if maxOps <= 0 {
		maxOps = 1
	}
	chunks := make([][]preparedPod, 0)
	current := make([]preparedPod, 0, maxOps)
	currentBytes := 0
	for _, pod := range pods {
		size := len(pod.key) + len(pod.value)
		if len(current) > 0 && (len(current) >= maxOps || currentBytes+size > w.config.MaxRequestBytes) {
			chunks = append(chunks, current)
			current = make([]preparedPod, 0, maxOps)
			currentBytes = 0
		}
		current = append(current, pod)
		currentBytes += size
	}
	if len(current) > 0 {
		chunks = append(chunks, current)
	}
	return chunks
}

func (w *EtcdWriter) createChunk(ctx context.Context, chunk []preparedPod, attempt int) []PodResult {
	kvs := make([]KeyValue, len(chunk))
	for i := range chunk {
		kvs[i] = KeyValue{Key: chunk[i].key, Value: chunk[i].value}
	}
	created, err := w.kv.CreateBatch(ctx, kvs)
	if err != nil {
		transient := isTransientError(err)
		results := make([]PodResult, len(chunk))
		for i := range chunk {
			results[i] = failedResult(chunk[i], err, transient)
		}
		return results
	}
	if created {
		results := make([]PodResult, len(chunk))
		for i := range chunk {
			results[i] = PodResult{
				Namespace: chunk[i].namespace,
				Name:      chunk[i].name,
				Status:    Created,
				Pod:       chunk[i].pod,
			}
		}
		return results
	}

	if attempt >= 2 {
		results := make([]PodResult, len(chunk))
		for i := range chunk {
			results[i] = failedResult(chunk[i], fmt.Errorf("etcd compare failed"), false)
		}
		return results
	}

	results := make([]PodResult, len(chunk))
	retry := make([]preparedPod, 0)
	retryIndexes := make([]int, 0)
	for i, pod := range chunk {
		value, getErr := w.kv.Get(ctx, pod.key)
		if getErr != nil {
			results[i] = failedResult(pod, getErr, isTransientError(getErr))
			continue
		}
		if value == nil {
			retry = append(retry, pod)
			retryIndexes = append(retryIndexes, i)
			continue
		}
		existing, decodeErr := w.encoder.DecodePod(value)
		if decodeErr != nil {
			results[i] = PodResult{
				Namespace: pod.namespace,
				Name:      pod.name,
				Status:    Rejected,
				Reason:    "existing pod cannot be decoded: " + decodeErr.Error(),
				Pod:       pod.pod,
			}
			continue
		}
		if pod.idempotencyKey != "" && existing.Labels[domain.JobRunId] == pod.idempotencyKey {
			results[i] = PodResult{
				Namespace: pod.namespace,
				Name:      pod.name,
				Status:    AlreadyExists,
				Pod:       existing,
			}
		} else {
			results[i] = PodResult{
				Namespace: pod.namespace,
				Name:      pod.name,
				Status:    Rejected,
				Reason:    "pod already exists",
				Pod:       existing,
			}
		}
	}
	if len(retry) > 0 {
		retryResults := w.createChunk(ctx, retry, attempt+1)
		for i, result := range retryResults {
			results[retryIndexes[i]] = result
		}
	}
	return results
}

func failedResult(pod preparedPod, err error, transient bool) PodResult {
	return PodResult{
		Namespace: pod.namespace,
		Name:      pod.name,
		Status:    Failed,
		Reason:    err.Error(),
		Transient: transient,
		Pod:       pod.pod,
	}
}

func isTransientError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted:
		return true
	default:
		return false
	}
}
