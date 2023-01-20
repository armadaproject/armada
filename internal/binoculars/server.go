package binoculars

import (
	"os"
	"sync"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/binoculars/configuration"
	"github.com/armadaproject/armada/internal/binoculars/logs"
	"github.com/armadaproject/armada/internal/binoculars/server"
	"github.com/armadaproject/armada/internal/common/auth"
	"github.com/armadaproject/armada/internal/common/cluster"
	grpcCommon "github.com/armadaproject/armada/internal/common/grpc"
	"github.com/armadaproject/armada/pkg/api/binoculars"
)

func StartUp(config *configuration.BinocularsConfig) (func(), *sync.WaitGroup) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	kubernetesClientProvider, err := cluster.NewKubernetesClientProvider(
		config.ImpersonateUsers,
		config.Kubernetes.QPS,
		config.Kubernetes.Burst,
	)
	if err != nil {
		log.Errorf("Failed to connect to kubernetes because %s", err)
		os.Exit(-1)
	}

	grpcServer := grpcCommon.CreateGrpcServer(config.Grpc.KeepaliveParams, config.Grpc.KeepaliveEnforcementPolicy, auth.ConfigureAuth(config.Auth))

	logService := logs.NewKubernetesLogService(kubernetesClientProvider)
	binocularsServer := server.NewBinocularsServer(logService)
	binoculars.RegisterBinocularsServer(grpcServer, binocularsServer)
	grpc_prometheus.Register(grpcServer)

	grpcCommon.Listen(config.GrpcPort, grpcServer, wg)

	return grpcServer.GracefulStop, wg
}
