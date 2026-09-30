package service

import (
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	authorizationv1 "k8s.io/api/authorization/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/auth"
	"github.com/armadaproject/armada/internal/common/cluster"
	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/pkg/api/binoculars"
)

type LogService interface {
	GetLogs(ctx *armadacontext.Context, params *LogParams) ([]*binoculars.LogLine, error)
}

type LogParams struct {
	Principal auth.Principal
	Namespace string
	// PodName is the pod to read. GetLogs ignores it when RunId is set.
	PodName    string
	JobId      string
	RunId      string
	SinceTime  string
	LogOptions *v1.PodLogOptions
}

type KubernetesLogService struct {
	clientProvider cluster.KubernetesClientProvider
}

const MaxLogBytes = 2000000

func NewKubernetesLogService(clientProvider cluster.KubernetesClientProvider) *KubernetesLogService {
	return &KubernetesLogService{clientProvider: clientProvider}
}

func (l *KubernetesLogService) GetLogs(ctx *armadacontext.Context, params *LogParams) ([]*binoculars.LogLine, error) {
	client, err := l.clientProvider.ClientForUser(params.Principal.GetName(), params.Principal.GetGroupNames())
	if err != nil {
		return nil, err
	}

	since, err := time.Parse(time.RFC3339Nano, params.SinceTime)
	if err == nil {
		params.LogOptions.SinceTime = &metav1.Time{Time: since}
	} else {
		if params.SinceTime != "" {
			log.Warnf("failed to parse since time for pod %s: %v", params.PodName, err)
		}
	}

	limitBytes := int64(MaxLogBytes)
	params.LogOptions.Follow = false
	params.LogOptions.Timestamps = true
	params.LogOptions.LimitBytes = &limitBytes

	if params.Namespace == "" {
		params.Namespace = "default"
	}

	podName := params.PodName
	if params.RunId != "" {
		if err := canReadLogs(ctx, client, params.Namespace); err != nil {
			return nil, err
		}
		podName, err = l.findPodName(ctx, params.Namespace, params.JobId, params.RunId)
		if err != nil {
			return nil, err
		}
	}

	req := client.CoreV1().
		Pods(params.Namespace).
		GetLogs(podName, params.LogOptions)

	result := req.Do(ctx)
	if err := result.Error(); err != nil {
		if errors.IsNotFound(err) {
			return nil, status.Error(codes.NotFound, "The pod with these logs doesn't exist - this is likely because the job has finished and the pod has been cleaned up")
		}
		return nil, err
	}

	rawLog, err := result.Raw()
	if err != nil {
		return nil, err
	}

	logLines, errs := ConvertLogs(rawLog)
	for _, err := range errs {
		log.Errorf(
			"failed to parse log line for namespace: %q, pod: %q: %v",
			params.Namespace,
			podName,
			err)
	}

	return logLines, nil
}

// canReadLogs checks that the user may read pod logs in the namespace. Binoculars looks up the pod only after
// this check. The lookup then never reveals pods to a user who cannot read their logs. Binoculars does not
// cache the result, so a removed permission takes effect on the next request.
func canReadLogs(ctx *armadacontext.Context, client kubernetes.Interface, namespace string) error {
	review, err := client.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx, &authorizationv1.SelfSubjectAccessReview{
		Spec: authorizationv1.SelfSubjectAccessReviewSpec{
			ResourceAttributes: &authorizationv1.ResourceAttributes{
				Namespace:   namespace,
				Verb:        "get",
				Resource:    "pods",
				Subresource: "log",
			},
		},
	}, metav1.CreateOptions{})
	if errors.IsForbidden(err) {
		return status.Errorf(codes.PermissionDenied, "not allowed to check log permission in namespace %s: %v", namespace, err)
	}
	if err != nil {
		return status.Errorf(codes.Internal, "failed to check log permission in namespace %s: %v", namespace, err)
	}
	if !review.Status.Allowed {
		return status.Errorf(codes.PermissionDenied, "not allowed to read pod logs in namespace %s", namespace)
	}
	return nil
}

// findPodName finds the pod of a run by its labels, so binoculars does not depend on the pod name.
// The lookup uses the binoculars service account, and GetLogs then reads the logs with the client of the user.
// Any pod in the namespace can carry these labels, so more than one match is an error. Binoculars does not
// cache the pod name, because a later run can reuse a job-scoped pod name.
func (l *KubernetesLogService) findPodName(ctx *armadacontext.Context, namespace, jobId, runId string) (string, error) {
	selector, err := labels.ValidatedSelectorFromSet(labels.Set{domain.JobId: jobId, domain.JobRunId: runId})
	if err != nil {
		return "", status.Errorf(codes.InvalidArgument, "invalid job ID or run ID: %v", err)
	}
	// The first list reads the watch cache of the API server, which is in memory. The watch cache can be behind,
	// so a new pod can be missing from it. A second list without a resource version then reads etcd.
	pods, err := l.listPods(ctx, namespace, selector.String(), "0")
	if err == nil && len(pods) == 0 {
		pods, err = l.listPods(ctx, namespace, selector.String(), "")
	}
	if err != nil {
		return "", err
	}
	switch len(pods) {
	case 0:
		return "", status.Errorf(codes.NotFound, "no pod exists for run %s of job %s", runId, jobId)
	case 1:
		return pods[0].Name, nil
	default:
		return "", status.Errorf(codes.FailedPrecondition, "%d pods exist for run %s of job %s, expected one", len(pods), runId, jobId)
	}
}

func (l *KubernetesLogService) listPods(ctx *armadacontext.Context, namespace string, selector string, resourceVersion string) ([]v1.Pod, error) {
	pods, err := l.clientProvider.Client().CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector:   selector,
		ResourceVersion: resourceVersion,
	})
	if err != nil {
		return nil, err
	}
	return pods.Items, nil
}

func ConvertLogs(rawLog []byte) ([]*binoculars.LogLine, []error) {
	lines := strings.Split(string(rawLog), "\n")
	// If log is larger than MAX_PAYLOAD_SIZE, discard last lines until it is smaller or equal to MAX_PAYLOAD_SIZE
	if len(rawLog) > MaxLogBytes {
		lines = truncateLog(lines, len(rawLog))
	}

	var logLines []*binoculars.LogLine
	var errs []error
	for i := 0; i < len(lines); i++ {
		line := lines[i]
		if line == "" { // Can happen if we have a trailing newline
			continue
		}

		logLine, err := splitLine(lines[i])
		if err != nil {
			errs = append(errs, err)
			continue
		}
		logLines = append(logLines, logLine)
	}

	return logLines, errs
}

func truncateLog(lines []string, total int) []string {
	lastExclIndex := len(lines)
	for total > MaxLogBytes {
		lastLine := lines[lastExclIndex-1]
		total -= len(lastLine) + 1 // newline removed with strings.Split
		lastExclIndex--
	}
	return lines[:lastExclIndex]
}

func splitLine(rawLine string) (*binoculars.LogLine, error) {
	spaceIdx := strings.Index(rawLine, " ")
	if spaceIdx == -1 {
		return nil, fmt.Errorf("badly formatted log line: %q", rawLine)
	}

	timestamp := rawLine[:spaceIdx]
	line := rawLine[spaceIdx+1:]

	_, err := time.Parse(time.RFC3339Nano, timestamp)
	if err != nil {
		return nil, fmt.Errorf("failed parse timestamp in log line: %q: %v", rawLine, err)
	}

	return &binoculars.LogLine{Timestamp: timestamp, Line: line}, nil
}
