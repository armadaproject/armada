package binoculars

import (
	"os"
	"sync"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/binoculars/configuration"
	"github.com/G-Research/armada/internal/binoculars/server"
	"github.com/G-Research/armada/internal/common/auth"
	"github.com/G-Research/armada/internal/common/cluster"
	grpcCommon "github.com/G-Research/armada/internal/common/grpc"
	"github.com/G-Research/armada/pkg/api/binoculars"
)

func StartUp(config *configuration.BinocularsConfig) (func(), *sync.WaitGroup) {

	wg := &sync.WaitGroup{}
	wg.Add(1)

	kubernetesClientProvider, err := cluster.NewKubernetesClientProvider(config.ImpersonateUsers)
	if err != nil {
		log.Errorf("Failed to connect to kubernetes because %s", err)
		os.Exit(-1)
	}

	grpcServer := grpcCommon.CreateGrpcServer(auth.ConfigureAuth(config.Auth))

	binocularsServer := server.NewBinocularsServer(kubernetesClientProvider)
	binoculars.RegisterBinocularsServer(grpcServer, binocularsServer)
	grpc_prometheus.Register(grpcServer)

	grpcCommon.Listen(config.GrpcPort, grpcServer, wg)

	return grpcServer.GracefulStop, wg
}
