package server

import (
	"context"
	"strconv"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/common/auth/authorization"
	"github.com/G-Research/armada/internal/common/cluster"
	"github.com/G-Research/armada/pkg/api/binoculars"
)

type BinocularsServer struct {
	clientProvider cluster.KubernetesClientProvider
}

func NewBinocularsServer(clientProvider cluster.KubernetesClientProvider) *BinocularsServer {
	return &BinocularsServer{clientProvider: clientProvider}
}

func (b BinocularsServer) Logs(ctx context.Context, request *binoculars.LogRequest) (*binoculars.LogResponse, error) {
	principal := authorization.GetPrincipal(ctx)
	client, err := b.clientProvider.ClientForUser(principal.GetName(), principal.GetGroupNames())
	if err != nil {
		return nil, err
	}

	since, err := time.Parse(time.RFC3339Nano, request.SinceTime)
	if err == nil {
		request.LogOptions.SinceTime = &metav1.Time{since}
	}
	request.LogOptions.Follow = false

	if request.PodNamespace == "" {
		request.PodNamespace = "default"
	}

	req := client.CoreV1().
		Pods(request.PodNamespace).
		GetLogs(common.PodNamePrefix+request.JobId+"-"+strconv.Itoa(int(request.PodNumber)), request.LogOptions)

	result := req.Do(ctx)
	if result.Error() != nil {
		return nil, result.Error()
	}
	data, err := result.Raw()
	if err != nil {
		return nil, err
	}

	return &binoculars.LogResponse{
		Log: string(data),
	}, nil
}
