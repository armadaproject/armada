package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/podwriter"
)

var (
	mode      = flag.String("mode", "etcd", "write path: etcd or api")
	endpoints = flag.String("endpoints", "https://127.0.0.1:2379", "comma-separated etcd endpoints")
	caFile    = flag.String("ca", "", "etcd CA certificate")
	certFile  = flag.String("cert", "", "etcd client certificate")
	keyFile   = flag.String("key", "", "etcd client key")
	prefix    = flag.String("prefix", "/registry", "etcd key prefix")

	namespace      = flag.String("namespace", "default", "pod namespace")
	namePrefix     = flag.String("name-prefix", "armada-bulk-bench", "pod name prefix")
	count          = flag.Int("count", 1, "number of pods to write")
	concurrency    = flag.Int("concurrency", 1, "number of concurrent batches")
	batchSize      = flag.Int("batch-size", 64, "pods per etcd transaction")
	txnConcurrency = flag.Int("txn-concurrency", 1, "etcd transactions committed in parallel per batch")
	maxRequestSize = flag.Int("max-request-bytes", 1572864, "maximum etcd transaction size")

	image         = flag.String("image", "registry.k8s.io/pause:3.9", "container image")
	nodeName      = flag.String("node-name", "", "bind pods to this node")
	nodeIdLabel   = flag.String("node-id-label", "kubernetes.io/hostname", "node selector label")
	schedulerName = flag.String("scheduler-name", "", "pod schedulerName; empty uses default-scheduler")

	dryRun     = flag.Bool("dry-run", false, "prepare and encode only, do not write to etcd")
	verify     = flag.Bool("verify", false, "verify the first written pod through the Kubernetes API")
	kubeconfig = flag.String("kubeconfig", "", "kubeconfig for verification")
	timeout    = flag.Duration("timeout", 10*time.Minute, "overall timeout")
	output     = flag.String("output", "", "write a JSON summary to this file")
)

func main() {
	flag.Parse()
	if *count <= 0 {
		fatal("-count must be positive")
	}
	if *concurrency <= 0 {
		fatal("-concurrency must be positive")
	}
	if *batchSize <= 0 {
		fatal("-batch-size must be positive")
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	pods := make([]*v1.Pod, *count)
	for i := range pods {
		pods[i] = makePod(i)
	}
	prepareOptions := podwriter.PrepareOptions{
		SchedulerName: *schedulerName,
		NodeIdLabel:   *nodeIdLabel,
	}

	if *dryRun {
		dryRunPods(pods, prepareOptions)
		return
	}

	switch *mode {
	case "api":
		start := time.Now()
		results := runAPI(ctx, pods)
		finish(ctx, results, time.Since(start))
		return
	case "etcd":
		if *prefix == "/registry" {
			fmt.Fprintln(os.Stderr, "warning: writing to /registry/pods; these become real pod objects")
		}

		client, err := podwriter.NewEtcdClient(podwriter.EtcdClientConfig{
			Endpoints:   splitEndpoints(*endpoints),
			CAFile:      *caFile,
			CertFile:    *certFile,
			KeyFile:     *keyFile,
			DialTimeout: 10 * time.Second,
		})
		if err != nil {
			fatal("create etcd client: %v", err)
		}
		defer client.Close()

		writer := podwriter.NewEtcdWriter(
			podwriter.NewEtcdKV(client),
			podwriter.Config{
				Prefix:          *prefix,
				MaxTxnOps:       *batchSize * 2,
				MaxRequestBytes: *maxRequestSize,
				Concurrency:     *txnConcurrency,
			},
			prepareOptions,
			nil,
		)

		// Pre-warm the gRPC connection so the first batch doesn't pay dial cost.
		warmCtx, warmCancel := context.WithTimeout(ctx, 10*time.Second)
		_, _ = podwriter.NewEtcdKV(client).Get(warmCtx, "/warmup")
		warmCancel()

		start := time.Now()
		results := runConcurrent(ctx, writer, pods)
		finish(ctx, results, time.Since(start))
	default:
		fatal("unknown -mode %q; use api or etcd", *mode)
	}
}

func finish(ctx context.Context, results []podwriter.PodResult, elapsed time.Duration) {
	summary := summarize(results, elapsed)
	printSummary(summary)
	if *output != "" {
		if err := writeJSON(*output, summary); err != nil {
			fatal("write output: %v", err)
		}
	}
	if summary.Rejected > 0 || summary.Failed > 0 {
		os.Exit(1)
	}
	if !*verify {
		return
	}
	if *mode == "etcd" && *prefix != "/registry" {
		fmt.Fprintf(os.Stderr, "warning: -verify requires -prefix=/registry; skipping\n")
		return
	}
	name := ""
	for _, result := range results {
		if result.Name != "" {
			name = result.Name
			break
		}
	}
	if name == "" {
		fatal("verify: no pod name in results")
	}
	if err := verifyPod(ctx, name); err != nil {
		fatal("verify pod: %v", err)
	}
}

func makePod(index int) *v1.Pod {
	name := fmt.Sprintf("%s-%05d", *namePrefix, index)
	requests := v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("100m"),
		v1.ResourceMemory: resource.MustParse("64Mi"),
	}
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: *namespace,
			Labels: map[string]string{
				domain.JobRunId: name,
			},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{{
				Name:  "main",
				Image: *image,
				Resources: v1.ResourceRequirements{
					Requests: requests,
					Limits:   requests.DeepCopy(),
				},
			}},
			RestartPolicy: v1.RestartPolicyNever,
		},
	}
	if *nodeName != "" {
		pod.Spec.NodeName = *nodeName
	}
	return pod
}

func dryRunPods(pods []*v1.Pod, options podwriter.PrepareOptions) {
	encoder := podwriter.NewEncoder()
	totalBytes := 0
	for _, pod := range pods {
		prepared := pod.DeepCopy()
		if err := podwriter.Prepare(prepared, options); err != nil {
			fatal("prepare %s: %v", pod.Name, err)
		}
		if err := podwriter.Validate(prepared); err != nil {
			fatal("validate %s: %v", pod.Name, err)
		}
		value, err := encoder.EncodePod(prepared)
		if err != nil {
			fatal("encode %s: %v", pod.Name, err)
		}
		totalBytes += len(value)
	}
	fmt.Printf("prepared %d pods, %d bytes total, %.0f bytes/pod\n", len(pods), totalBytes, float64(totalBytes)/float64(len(pods)))
}

func runConcurrent(ctx context.Context, writer podwriter.Writer, pods []*v1.Pod) []podwriter.PodResult {
	batches := *concurrency
	if batches > len(pods) {
		batches = len(pods)
	}
	perBatch := (len(pods) + batches - 1) / batches

	var wg sync.WaitGroup
	var mutex sync.Mutex
	results := make([]podwriter.PodResult, 0, len(pods))
	for start := 0; start < len(pods); start += perBatch {
		end := start + perBatch
		if end > len(pods) {
			end = len(pods)
		}
		wg.Add(1)
		go func(batch []*v1.Pod) {
			defer wg.Done()
			request := podwriter.BatchRequest{
				User: "bulk-bench",
				Pods: make([]podwriter.PodRequest, len(batch)),
			}
			for i, pod := range batch {
				request.Pods[i] = podwriter.PodRequest{
					Namespace:      pod.Namespace,
					IdempotencyKey: pod.Name,
					Pod:            pod,
				}
			}
			result := writer.CreateBatch(ctx, request)
			mutex.Lock()
			results = append(results, result.Results...)
			mutex.Unlock()
		}(pods[start:end])
	}
	wg.Wait()
	return results
}

func runAPI(ctx context.Context, pods []*v1.Pod) []podwriter.PodResult {
	client, err := kubeClient()
	if err != nil {
		fatal("create Kubernetes client: %v", err)
	}
	results := make([]podwriter.PodResult, len(pods))
	semaphore := make(chan struct{}, *concurrency)
	var wg sync.WaitGroup
	for i, pod := range pods {
		wg.Add(1)
		go func(index int, pod *v1.Pod) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()
			_, createErr := client.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{})
			result := podwriter.PodResult{Namespace: pod.Namespace, Name: pod.Name, Pod: pod}
			switch {
			case createErr == nil:
				result.Status = podwriter.Created
			case k8s_errors.IsAlreadyExists(createErr):
				result.Status = podwriter.AlreadyExists
			default:
				result.Status = podwriter.Failed
				result.Reason = createErr.Error()
				result.Transient = k8s_errors.IsServerTimeout(createErr) ||
					k8s_errors.IsTooManyRequests(createErr) ||
					k8s_errors.IsServiceUnavailable(createErr)
			}
			results[index] = result
		}(i, pod)
	}
	wg.Wait()
	return results
}

func kubeClient() (*kubernetes.Clientset, error) {
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(),
		&clientcmd.ConfigOverrides{},
	).ClientConfig()
	if *kubeconfig != "" {
		config, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
	}
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig: %w", err)
	}
	// Benchmarks use high parallelism; the 5-QPS client-go default would
	// throttle the client, not the server.
	config.QPS = 5000
	config.Burst = 5000
	return kubernetes.NewForConfig(config)
}

type summary struct {
	Count           int      `json:"count"`
	Created         int      `json:"created"`
	AlreadyExists   int      `json:"alreadyExists"`
	Rejected        int      `json:"rejected"`
	Failed          int      `json:"failed"`
	TransientFailed int      `json:"transientFailed"`
	DurationSeconds float64  `json:"durationSeconds"`
	PodsPerSecond   float64  `json:"podsPerSecond"`
	Errors          []string `json:"errors,omitempty"`
}

func summarize(results []podwriter.PodResult, elapsed time.Duration) summary {
	summary := summary{
		Count:           len(results),
		DurationSeconds: elapsed.Seconds(),
	}
	for _, result := range results {
		switch result.Status {
		case podwriter.Created:
			summary.Created++
		case podwriter.AlreadyExists:
			summary.AlreadyExists++
		case podwriter.Rejected:
			summary.Rejected++
		case podwriter.Failed:
			summary.Failed++
			if result.Transient {
				summary.TransientFailed++
			}
		}
		if result.Status != podwriter.Created && result.Status != podwriter.AlreadyExists && len(summary.Errors) < 20 {
			summary.Errors = append(summary.Errors, fmt.Sprintf("%s/%s: %s", result.Namespace, result.Name, result.Reason))
		}
	}
	if elapsed > 0 {
		summary.PodsPerSecond = float64(len(results)) / elapsed.Seconds()
	}
	return summary
}

func printSummary(summary summary) {
	fmt.Printf("pods=%d created=%d alreadyExists=%d rejected=%d failed=%d transient=%d\n",
		summary.Count, summary.Created, summary.AlreadyExists, summary.Rejected, summary.Failed, summary.TransientFailed)
	fmt.Printf("duration=%.3fs pods/s=%.1f\n", summary.DurationSeconds, summary.PodsPerSecond)
	for _, message := range summary.Errors {
		fmt.Fprintf(os.Stderr, "error: %s\n", message)
	}
}

func writeJSON(path string, summary summary) error {
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}

func verifyPod(ctx context.Context, name string) error {
	client, err := kubeClient()
	if err != nil {
		return err
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		pod, getErr := client.CoreV1().Pods(*namespace).Get(ctx, name, metav1.GetOptions{})
		if getErr == nil {
			fmt.Printf("verified: pod=%s phase=%s node=%s uid=%s\n", pod.Name, pod.Status.Phase, pod.Spec.NodeName, pod.UID)
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("pod %s not visible through the API server: %w", name, getErr)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func splitEndpoints(value string) []string {
	parts := strings.Split(value, ",")
	endpoints := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			endpoints = append(endpoints, trimmed)
		}
	}
	return endpoints
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
