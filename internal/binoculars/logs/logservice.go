package logs

import (
	"context"
	"github.com/G-Research/armada/internal/common/auth/authorization"
	"github.com/G-Research/armada/internal/common/cluster"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strings"
	"time"
)

type LogService interface {
	GetLogs(ctx context.Context, params *LogParams) ([]*LogLine, error)
}

type LogParams struct {
	Principal  authorization.Principal
	Namespace  string
	PodName    string
	SinceTime  string
	LogOptions *v1.PodLogOptions
}

type LogLine struct {
	timestamp string
	log       string
}

type KubernetesLogService struct {
	clientProvider cluster.KubernetesClientProvider
}

const MAX_PAYLOAD_SIZE = 2000000

func NewKubernetesLogService(clientProvider cluster.KubernetesClientProvider) *KubernetesLogService {
	return &KubernetesLogService{clientProvider: clientProvider}
}

func (l *KubernetesLogService) GetLogs(ctx context.Context, params *LogParams) ([]*LogLine, error) {
	client, err := l.clientProvider.ClientForUser(params.Principal.GetName(), params.Principal.GetGroupNames())
	if err != nil {
		return nil, err
	}

	since, err := time.Parse(time.RFC3339Nano, params.SinceTime)
	if params.SinceTime != "" && err == nil {
		params.LogOptions.SinceTime = &metav1.Time{Time: since}
	}
	params.LogOptions.Follow = false

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

	data, err := result.Raw()
	if err != nil {
		return nil, err
	}
}

func ConvertLogs(data []byte) []*LogLine {
	lines := strings.Split(string(data), "\n")
	if len(data) > MAX_PAYLOAD_SIZE {

	}
}
