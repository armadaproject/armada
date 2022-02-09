package logs

import (
	"context"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/G-Research/armada/internal/common/auth/authorization"
	"github.com/G-Research/armada/internal/common/cluster"
	"github.com/G-Research/armada/pkg/api/binoculars"
)

type LogService interface {
	GetLogs(ctx context.Context, params *LogParams) ([]*binoculars.LogLine, error)
}

type LogParams struct {
	Principal  authorization.Principal
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

func (l *KubernetesLogService) GetLogs(ctx context.Context, params *LogParams) ([]*binoculars.LogLine, error) {
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
	if result.Error() != nil {
		return nil, result.Error()
	}

	rawLog, err := result.Raw()
	if err != nil {
		return nil, err
	}

	return ConvertLogs(rawLog), nil
}

func ConvertLogs(rawLog []byte) []*binoculars.LogLine {
	lines := strings.Split(string(rawLog), "\n")
	// If log is larger than MAX_PAYLOAD_SIZE, discard last lines until it is smaller or equal to MAX_PAYLOAD_SIZE
	if len(rawLog) > MaxLogBytes {
		lines = truncateLog(lines, len(rawLog))
	}

	var logLines []*binoculars.LogLine
	for i := 0; i < len(lines); i++ {
		if lines[i] != "" {
			logLines = append(logLines, splitLine(lines[i]))
		}
	}

	return logLines
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

func splitLine(rawLine string) *binoculars.LogLine {
	spaceIdx := strings.Index(rawLine, " ")

	if spaceIdx == -1 {
		return &binoculars.LogLine{Timestamp: "", Line: rawLine}
	}

	timestamp := rawLine[:spaceIdx]
	line := rawLine[spaceIdx+1:]

	_, err := time.Parse(time.RFC3339Nano, timestamp)
	if err != nil {
		return &binoculars.LogLine{Timestamp: "", Line: rawLine}
	}

	return &binoculars.LogLine{Timestamp: timestamp, Line: line}
}
