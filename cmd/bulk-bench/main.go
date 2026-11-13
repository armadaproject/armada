package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
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
	mode      = flag.String("mode", "etcd", "write path: etcd, api, or delete")
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

	rateLimit           = flag.Float64("rate", 0, "target creates per second (api mode); 0 means unpaced, max throughput")
	podAnnotationsBytes = flag.Int("pod-annotations-bytes", 0, "pad each pod with one annotation of this many bytes, to vary pod object size")
	collectMetrics      = flag.Bool("apiserver-metrics", true, "scrape apiserver /metrics to measure control-plane impact (api mode)")
	metricsInterval     = flag.Duration("metrics-interval", 2*time.Second, "apiserver inflight sampling interval")
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
	if *rateLimit < 0 {
		fatal("-rate must not be negative")
	}
	// 256KiB is the apiserver's total annotation size limit; keep the single
	// padding annotation well below it so the pod is admitted.
	if *podAnnotationsBytes < 0 || *podAnnotationsBytes > 200*1024 {
		fatal("-pod-annotations-bytes must be between 0 and 204800")
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	prepareOptions := podwriter.PrepareOptions{
		SchedulerName: *schedulerName,
		NodeIdLabel:   *nodeIdLabel,
	}

	if *dryRun {
		dryRunPods(*count, prepareOptions)
		return
	}

	switch *mode {
	case "api":
		client, err := kubeClient()
		if err != nil {
			fatal("create Kubernetes client: %v", err)
		}
		controlPlane := newApiserverMetrics(client, *collectMetrics, *metricsInterval)
		controlPlane.Start()
		start := time.Now()
		results, latencies := runAPI(ctx, client, *count)
		elapsed := time.Since(start)
		summary := summarize(results, latencies, elapsed)
		summary.Apiserver = controlPlane.Stop()
		finish(ctx, summary)
		return
	case "delete":
		// Parallel cleanup: the namespace controller deletes pods at roughly
		// 10/s, which makes a 20k batch take half an hour to disappear.
		client, err := kubeClient()
		if err != nil {
			fatal("create Kubernetes client: %v", err)
		}
		start := time.Now()
		results, latencies := runDelete(ctx, client, *count)
		finish(ctx, summarize(results, latencies, time.Since(start)))
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
		results := runConcurrent(ctx, writer, *count)
		finish(ctx, summarize(results, nil, time.Since(start)))
	default:
		fatal("unknown -mode %q; use api or etcd", *mode)
	}
}

func finish(ctx context.Context, summary summary) {
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
	if err := verifyPod(ctx, makePod(0).Name); err != nil {
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
	// Real Armada pods carry labels, annotations and env; padding one
	// annotation is a cheap proxy for object size on the write path.
	if *podAnnotationsBytes > 0 {
		pod.Annotations = map[string]string{"bench/padding": strings.Repeat("x", *podAnnotationsBytes)}
	}
	if *nodeName != "" {
		pod.Spec.NodeName = *nodeName
	}
	return pod
}

func makePods(start, end int) []*v1.Pod {
	pods := make([]*v1.Pod, end-start)
	for i := range pods {
		pods[i] = makePod(start + i)
	}
	return pods
}

func dryRunPods(count int, options podwriter.PrepareOptions) {
	encoder := podwriter.NewEncoder()
	totalBytes := 0
	for i := 0; i < count; i++ {
		prepared := makePod(i)
		if err := podwriter.Prepare(prepared, options); err != nil {
			fatal("prepare %s: %v", prepared.Name, err)
		}
		if err := podwriter.Validate(prepared); err != nil {
			fatal("validate %s: %v", prepared.Name, err)
		}
		value, err := encoder.EncodePod(prepared)
		if err != nil {
			fatal("encode %s: %v", prepared.Name, err)
		}
		totalBytes += len(value)
	}
	fmt.Printf("prepared %d pods, %d bytes total, %.0f bytes/pod\n", count, totalBytes, float64(totalBytes)/float64(count))
}

func runConcurrent(ctx context.Context, writer podwriter.Writer, count int) []podwriter.PodResult {
	// shortcut: every concurrency batch materializes its pods up front, so peak
	// memory is count*objectSize; chunk this if etcd mode ever needs 20k large pods.
	batches := *concurrency
	if batches > count {
		batches = count
	}
	perBatch := (count + batches - 1) / batches

	var wg sync.WaitGroup
	var mutex sync.Mutex
	results := make([]podwriter.PodResult, 0, count)
	for start := 0; start < count; start += perBatch {
		end := start + perBatch
		if end > count {
			end = count
		}
		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			batch := makePods(start, end)
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
		}(start, end)
	}
	wg.Wait()
	return results
}

// runAPI creates pods through the API server. Pods are generated inside the
// workers rather than up front: 20k large specs would otherwise allocate
// gigabytes and push GC into the measured window.
func runAPI(ctx context.Context, client kubernetes.Interface, count int) ([]podwriter.PodResult, []time.Duration) {
	results := make([]podwriter.PodResult, count)
	latencies := make([]time.Duration, count)
	// Burst 1 paces evenly; a larger burst lets a run front-load requests and
	// makes the measured rate look better than the sustained one.
	var limiter *rate.Limiter
	if *rateLimit > 0 {
		limiter = rate.NewLimiter(rate.Limit(*rateLimit), 1)
	}

	indexes := make(chan int)
	var wg sync.WaitGroup
	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range indexes {
				pod := makePod(index)
				result := podwriter.PodResult{Namespace: pod.Namespace, Name: pod.Name, Pod: pod}
				if limiter != nil {
					if err := limiter.Wait(ctx); err != nil {
						result.Status = podwriter.Failed
						result.Reason = err.Error()
						results[index] = result
						return
					}
				}
				started := time.Now()
				_, createErr := client.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{})
				latencies[index] = time.Since(started)
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
			}
		}()
	}
	for index := 0; index < count; index++ {
		select {
		case indexes <- index:
		case <-ctx.Done():
			close(indexes)
			wg.Wait()
			return results, latencies
		}
	}
	close(indexes)
	wg.Wait()
	return results, latencies
}

// runDelete removes the pods a -mode=api run created, in parallel. The
// namespace controller is far slower, so cleanup goes through the API directly;
// NotFound counts as success so a partial create run still cleans up.
func runDelete(ctx context.Context, client kubernetes.Interface, count int) ([]podwriter.PodResult, []time.Duration) {
	results := make([]podwriter.PodResult, count)
	latencies := make([]time.Duration, count)
	indexes := make(chan int)
	var wg sync.WaitGroup
	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range indexes {
				pod := makePod(index)
				started := time.Now()
				// Grace 0: pods are bound to a node that does not exist, so
				// nothing would ever confirm a graceful delete and they would
				// sit in Terminating for minutes.
				grace := int64(0)
				err := client.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name,
					metav1.DeleteOptions{GracePeriodSeconds: &grace})
				latencies[index] = time.Since(started)
				result := podwriter.PodResult{Namespace: pod.Namespace, Name: pod.Name, Pod: pod}
				switch {
				case err == nil, k8s_errors.IsNotFound(err):
					result.Status = podwriter.Created
				default:
					result.Status = podwriter.Failed
					result.Reason = err.Error()
				}
				results[index] = result
			}
		}()
	}
	for index := 0; index < count; index++ {
		select {
		case indexes <- index:
		case <-ctx.Done():
			close(indexes)
			wg.Wait()
			return results, latencies
		}
	}
	close(indexes)
	wg.Wait()
	return results, latencies
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
	Mode                string           `json:"mode"`
	Namespace           string           `json:"namespace"`
	Count               int              `json:"count"`
	Concurrency         int              `json:"concurrency"`
	RateLimit           float64          `json:"rateLimit"`
	PodAnnotationsBytes int              `json:"podAnnotationsBytes"`
	Created             int              `json:"created"`
	AlreadyExists       int              `json:"alreadyExists"`
	Rejected            int              `json:"rejected"`
	Failed              int              `json:"failed"`
	TransientFailed     int              `json:"transientFailed"`
	DurationSeconds     float64          `json:"durationSeconds"`
	PodsPerSecond       float64          `json:"podsPerSecond"`
	ClientMeanMs        float64          `json:"clientMeanMs"`
	ClientP50Ms         float64          `json:"clientP50Ms"`
	ClientP90Ms         float64          `json:"clientP90Ms"`
	ClientP99Ms         float64          `json:"clientP99Ms"`
	ClientMaxMs         float64          `json:"clientMaxMs"`
	Apiserver           *apiserverReport `json:"apiserver,omitempty"`
	Errors              []string         `json:"errors,omitempty"`
}

func summarize(results []podwriter.PodResult, latencies []time.Duration, elapsed time.Duration) summary {
	summary := summary{
		Mode:                *mode,
		Namespace:           *namespace,
		Count:               len(results),
		Concurrency:         *concurrency,
		RateLimit:           *rateLimit,
		PodAnnotationsBytes: *podAnnotationsBytes,
		DurationSeconds:     elapsed.Seconds(),
	}
	var created []time.Duration
	for index, result := range results {
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
		if result.Status == podwriter.Created && index < len(latencies) {
			created = append(created, latencies[index])
		}
	}
	// Insert rate counts pods that actually landed, so a run that mostly got
	// 429s cannot look fast.
	if elapsed > 0 {
		summary.PodsPerSecond = float64(summary.Created) / elapsed.Seconds()
	}
	summary.ClientMeanMs, summary.ClientP50Ms, summary.ClientP90Ms, summary.ClientP99Ms, summary.ClientMaxMs = latencyStats(created)
	return summary
}

func latencyStats(latencies []time.Duration) (mean, p50, p90, p99, maxMs float64) {
	if len(latencies) == 0 {
		return 0, 0, 0, 0, 0
	}
	sorted := append([]time.Duration(nil), latencies...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var total time.Duration
	for _, d := range sorted {
		total += d
	}
	return float64(total.Microseconds()) / 1000 / float64(len(sorted)),
		percentileMs(sorted, 0.50),
		percentileMs(sorted, 0.90),
		percentileMs(sorted, 0.99),
		float64(sorted[len(sorted)-1].Microseconds()) / 1000
}

// percentileMs uses nearest-rank, which is accurate enough at 20k samples and
// avoids interpolation edge cases.
func percentileMs(sorted []time.Duration, p float64) float64 {
	index := int(math.Ceil(p*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return float64(sorted[index].Microseconds()) / 1000
}

func printSummary(summary summary) {
	fmt.Printf("pods=%d created=%d alreadyExists=%d rejected=%d failed=%d transient=%d\n",
		summary.Count, summary.Created, summary.AlreadyExists, summary.Rejected, summary.Failed, summary.TransientFailed)
	fmt.Printf("duration=%.3fs pods/s=%.1f\n", summary.DurationSeconds, summary.PodsPerSecond)
	if summary.Created > 0 {
		fmt.Printf("client latency: mean=%.1fms p50=%.1fms p90=%.1fms p99=%.1fms max=%.1fms\n",
			summary.ClientMeanMs, summary.ClientP50Ms, summary.ClientP90Ms, summary.ClientP99Ms, summary.ClientMaxMs)
	}
	if pressure := summary.Apiserver; pressure != nil {
		if !pressure.Enabled {
			fmt.Printf("apiserver metrics: unavailable (%s)\n", pressure.Reason)
		} else {
			fmt.Printf("apiserver: post=%d (%.0f/s) mean=%.1fms etcdCreate=%.1fms webhook=%.1fms inflightPeak=%.0f cpu=%.2fcores rss=%.0fMiB\n",
				int(pressure.PostRequests), pressure.PostRatePerSecond, pressure.PostMeanMs,
				pressure.EtcdCreateMeanMs, pressure.WebhookAdmitMeanMs, pressure.InflightMutatingPeak,
				pressure.CPUCoresMean, pressure.RSSAfterBytes/(1<<20))
			fmt.Printf("apiserver codes: 201=%d 429=%d 5xx=%d apfRejected=%d allRequests=%.0f/s podsInStorage=%d->%d dbSize=%.0fMiB->%.0fMiB\n",
				int(pressure.Code201), int(pressure.Code429), int(pressure.Code5xx), int(pressure.APFRejected),
				pressure.AllRequestsPerSecond, int(pressure.PodsInStorageBefore), int(pressure.PodsInStorageAfter),
				pressure.DBSizeBytesBefore/(1<<20), pressure.DBSizeBytesAfter/(1<<20))
		}
	}
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
