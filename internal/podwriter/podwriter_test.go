package podwriter

import (
	"context"
	"fmt"
	"sync"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/armadaproject/armada/internal/executor/domain"
)

var testPrepareOptions = PrepareOptions{
	SchedulerName: "default-scheduler",
	NodeIdLabel:   "kubernetes.io/hostname",
}

func testPod(name string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels: map[string]string{
				domain.JobRunId: "run-" + name,
			},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{{
				Name:  "main",
				Image: "registry.k8s.io/pause:3.9",
				Resources: v1.ResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("100m"),
						v1.ResourceMemory: resource.MustParse("64Mi"),
					},
					Limits: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("100m"),
						v1.ResourceMemory: resource.MustParse("64Mi"),
					},
				},
			}},
		},
	}
}

func TestPrepareAppliesDefaultsAndNode(t *testing.T) {
	pod := testPod("pod-1")
	pod.Spec.NodeSelector = map[string]string{"kubernetes.io/hostname": "node-1"}

	if err := Prepare(pod, testPrepareOptions); err != nil {
		t.Fatalf("Prepare returned error: %v", err)
	}
	if pod.UID == "" {
		t.Fatal("expected UID to be set")
	}
	if pod.ResourceVersion != "" {
		t.Fatalf("expected empty resourceVersion, got %q", pod.ResourceVersion)
	}
	if pod.Status.Phase != v1.PodPending {
		t.Fatalf("expected Pending phase, got %q", pod.Status.Phase)
	}
	if pod.Status.QOSClass != v1.PodQOSGuaranteed {
		t.Fatalf("expected Guaranteed QoS, got %q", pod.Status.QOSClass)
	}
	if pod.Spec.TerminationGracePeriodSeconds == nil || *pod.Spec.TerminationGracePeriodSeconds != 30 {
		t.Fatalf("expected 30s grace period, got %v", pod.Spec.TerminationGracePeriodSeconds)
	}
	if pod.Spec.DNSPolicy != v1.DNSClusterFirst {
		t.Fatalf("expected ClusterFirst DNS policy, got %q", pod.Spec.DNSPolicy)
	}
	if pod.Spec.ServiceAccountName != "default" {
		t.Fatalf("expected default service account, got %q", pod.Spec.ServiceAccountName)
	}
	if pod.Spec.NodeName != "node-1" {
		t.Fatalf("expected node-1, got %q", pod.Spec.NodeName)
	}
	if _, ok := pod.Spec.NodeSelector["kubernetes.io/hostname"]; ok {
		t.Fatal("expected node id selector to be removed")
	}
}

func TestValidateRejectsUnsupportedPods(t *testing.T) {
	valid := testPod("pod-1")
	if err := Prepare(valid, testPrepareOptions); err != nil {
		t.Fatalf("Prepare returned error: %v", err)
	}
	if err := Validate(valid); err != nil {
		t.Fatalf("expected valid pod, got %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*v1.Pod)
	}{
		{
			name: "privileged",
			mutate: func(pod *v1.Pod) {
				pod.Spec.Containers[0].SecurityContext = &v1.SecurityContext{Privileged: boolPtr(true)}
			},
		},
		{
			name: "host path",
			mutate: func(pod *v1.Pod) {
				pod.Spec.Volumes = []v1.Volume{{
					Name:         "host",
					VolumeSource: v1.VolumeSource{HostPath: &v1.HostPathVolumeSource{Path: "/"}},
				}}
			},
		},
		{
			name: "custom service account",
			mutate: func(pod *v1.Pod) {
				pod.Spec.ServiceAccountName = "custom"
			},
		},
		{
			name: "missing resources",
			mutate: func(pod *v1.Pod) {
				pod.Spec.Containers[0].Resources = v1.ResourceRequirements{}
			},
		},
		{
			name: "two containers",
			mutate: func(pod *v1.Pod) {
				pod.Spec.Containers = append(pod.Spec.Containers, pod.Spec.Containers[0])
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pod := testPod("pod-1")
			if err := Prepare(pod, testPrepareOptions); err != nil {
				t.Fatalf("Prepare returned error: %v", err)
			}
			test.mutate(pod)
			if err := Validate(pod); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestEncodeRoundTrip(t *testing.T) {
	pod := testPod("pod-1")
	if err := Prepare(pod, testPrepareOptions); err != nil {
		t.Fatalf("Prepare returned error: %v", err)
	}
	encoder := NewEncoder()
	value, err := encoder.EncodePod(pod)
	if err != nil {
		t.Fatalf("EncodePod returned error: %v", err)
	}
	if len(value) < 4 || value[0] != 'k' || value[1] != '8' || value[2] != 's' || value[3] != 0 {
		t.Fatalf("expected k8s protobuf prefix, got %x", value)
	}
	decoded, err := encoder.DecodePod(value)
	if err != nil {
		t.Fatalf("DecodePod returned error: %v", err)
	}
	if decoded.Name != pod.Name || decoded.Namespace != pod.Namespace {
		t.Fatalf("round trip mismatch: %s/%s", decoded.Namespace, decoded.Name)
	}
}

func TestCreateBatchChunksByTransactionOps(t *testing.T) {
	kv := newFakeKV()
	writer := NewEtcdWriter(kv, Config{MaxTxnOps: 4}, testPrepareOptions, nil) // 2 pods per txn
	request := BatchRequest{
		User: "test",
		Pods: []PodRequest{
			{Namespace: "default", IdempotencyKey: "run-pod-1", Pod: testPod("pod-1")},
			{Namespace: "default", IdempotencyKey: "run-pod-2", Pod: testPod("pod-2")},
			{Namespace: "default", IdempotencyKey: "run-pod-3", Pod: testPod("pod-3")},
		},
	}
	result := writer.CreateBatch(context.Background(), request)
	if len(result.Results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(result.Results))
	}
	for i, podResult := range result.Results {
		if podResult.Status != Created {
			t.Fatalf("result %d: expected Created, got %s (%s)", i, podResult.Status, podResult.Reason)
		}
	}
	if kv.calls != 2 {
		t.Fatalf("expected 2 transactions, got %d", kv.calls)
	}
}

func TestCreateBatchCommitsChunksInParallel(t *testing.T) {
	kv := newFakeKV()
	writer := NewEtcdWriter(kv, Config{MaxTxnOps: 4, Concurrency: 3}, testPrepareOptions, nil) // 2 pods per txn, 3 in flight
	pods := make([]PodRequest, 7)
	for i := range pods {
		pods[i] = PodRequest{Namespace: "default", IdempotencyKey: fmt.Sprintf("run-pod-%d", i), Pod: testPod(fmt.Sprintf("pod-%d", i))}
	}
	result := writer.CreateBatch(context.Background(), BatchRequest{User: "test", Pods: pods})
	if len(result.Results) != 7 {
		t.Fatalf("expected 7 results, got %d", len(result.Results))
	}
	for i, podResult := range result.Results {
		if podResult.Status != Created {
			t.Fatalf("result %d: expected Created, got %s (%s)", i, podResult.Status, podResult.Reason)
		}
	}
	if kv.calls != 4 {
		t.Fatalf("expected 4 transactions, got %d", kv.calls)
	}
}

func TestCreateBatchIsIdempotentForSameRun(t *testing.T) {
	kv := newFakeKV()
	encoder := NewEncoder()
	existing := testPod("pod-1")
	if err := Prepare(existing, testPrepareOptions); err != nil {
		t.Fatalf("Prepare returned error: %v", err)
	}
	value, err := encoder.EncodePod(existing)
	if err != nil {
		t.Fatalf("EncodePod returned error: %v", err)
	}
	kv.data[EncodeKey("/registry", "default", "pod-1")] = value

	writer := NewEtcdWriter(kv, Config{}, testPrepareOptions, nil)
	result := writer.CreateBatch(context.Background(), BatchRequest{
		Pods: []PodRequest{{
			Namespace:      "default",
			IdempotencyKey: "run-pod-1",
			Pod:            testPod("pod-1"),
		}},
	})
	if result.Results[0].Status != AlreadyExists {
		t.Fatalf("expected AlreadyExists, got %s (%s)", result.Results[0].Status, result.Results[0].Reason)
	}
}

func TestCreateBatchRejectsDifferentRun(t *testing.T) {
	kv := newFakeKV()
	encoder := NewEncoder()
	existing := testPod("pod-1")
	existing.Labels[domain.JobRunId] = "another-run"
	if err := Prepare(existing, testPrepareOptions); err != nil {
		t.Fatalf("Prepare returned error: %v", err)
	}
	value, err := encoder.EncodePod(existing)
	if err != nil {
		t.Fatalf("EncodePod returned error: %v", err)
	}
	kv.data[EncodeKey("/registry", "default", "pod-1")] = value

	writer := NewEtcdWriter(kv, Config{}, testPrepareOptions, nil)
	result := writer.CreateBatch(context.Background(), BatchRequest{
		Pods: []PodRequest{{
			Namespace:      "default",
			IdempotencyKey: "run-pod-1",
			Pod:            testPod("pod-1"),
		}},
	})
	if result.Results[0].Status != Rejected {
		t.Fatalf("expected Rejected, got %s (%s)", result.Results[0].Status, result.Results[0].Reason)
	}
}

func TestCreateBatchMarksTransientErrors(t *testing.T) {
	kv := newFakeKV()
	kv.createErr = context.DeadlineExceeded
	writer := NewEtcdWriter(kv, Config{}, testPrepareOptions, nil)
	result := writer.CreateBatch(context.Background(), BatchRequest{
		Pods: []PodRequest{{
			Namespace:      "default",
			IdempotencyKey: "run-pod-1",
			Pod:            testPod("pod-1"),
		}},
	})
	if result.Results[0].Status != Failed {
		t.Fatalf("expected Failed, got %s", result.Results[0].Status)
	}
	if !result.Results[0].Transient {
		t.Fatal("expected transient failure")
	}
}

type fakeKV struct {
	mutex     sync.Mutex
	data      map[string][]byte
	createErr error
	getErr    error
	calls     int
}

func newFakeKV() *fakeKV {
	return &fakeKV{data: map[string][]byte{}}
}

func (f *fakeKV) CreateBatch(_ context.Context, kvs []KeyValue) (bool, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.calls++
	if f.createErr != nil {
		return false, f.createErr
	}
	for _, kv := range kvs {
		if _, ok := f.data[kv.Key]; ok {
			return false, nil
		}
	}
	for _, kv := range kvs {
		f.data[kv.Key] = kv.Value
	}
	return true, nil
}

func (f *fakeKV) Get(_ context.Context, key string) ([]byte, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.data[key], nil
}

func boolPtr(value bool) *bool {
	return &value
}
