package service

import (
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/auth"
	"github.com/armadaproject/armada/internal/common/cluster"
	"github.com/armadaproject/armada/pkg/api/binoculars"
)

type LogService interface {
	GetLogs(ctx *armadacontext.Context, params *LogParams) ([]*binoculars.LogLine, error)
}

type LogParams struct {
	Principal  auth.Principal
	Namespace  string
	PodName    string
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

	req := client.CoreV1().
		Pods(params.Namespace).
		GetLogs(params.PodName, params.LogOptions)

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
			params.PodName,
			err)
	}

	return logLines, nil
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
