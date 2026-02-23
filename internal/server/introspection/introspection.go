package introspection

import (
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/server/introspection"
	"github.com/armadaproject/armada/pkg/api/introspection"
)

type IntrospectionServer struct {
	kube KubeClientFactory
	* introspection.UnimplementedIntrospectionServer
}

func NewIntrospectionServer(
	kube KubeClientFactory,
) *IntrospectionServer {
	return &IntrospectionServer{
		kube: kube,
	}
}

func (s *IntrospectionServer) GetJobLogs (request *introspection.GetJobLogsRequest, stream introspection.Introspection_GetJobLogsClient) error {
	ctx := armadacontext.FromGrpcCtx(stream.Context())
	q, err := s.
}