package binoculars

import (
	"fmt"
	"sync"

	"github.com/armadaproject/armada/internal/binoculars/configuration"
	"github.com/armadaproject/armada/internal/binoculars/server"
	"github.com/armadaproject/armada/internal/binoculars/service"
	"github.com/armadaproject/armada/internal/common/auth"
	"github.com/armadaproject/armada/internal/common/cluster"
	grpcCommon "github.com/armadaproject/armada/internal/common/grpc"
	"github.com/armadaproject/armada/pkg/api/binoculars"
)

func StartUp(config *configuration.BinocularsConfig) (func(), *sync.WaitGroup, error) {
	kubernetesClientProvider, err := cluster.NewKubernetesClientProvider(
		config.ImpersonateUsers,
		config.Kubernetes.QPS,
		config.Kubernetes.Burst,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to kubernetes: %w", err)
	}

	authServices, err := auth.ConfigureAuth(config.Auth)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create auth services: %w", err)
	}

	grpcServer := grpcCommon.CreateGrpcServer(config.Grpc.KeepaliveParams, config.Grpc.KeepaliveEnforcementPolicy, authServices, config.Grpc.Tls)

	permissionsChecker := auth.NewPrincipalPermissionChecker(
		config.Auth.PermissionGroupMapping,
		config.Auth.PermissionScopeMapping,
		config.Auth.PermissionClaimMapping,
	)

	logService := service.NewKubernetesLogService(kubernetesClientProvider)
	cordonService := service.NewKubernetesCordonService(config.Cordon, permissionsChecker, kubernetesClientProvider)
	binocularsServer := server.NewBinocularsServer(logService, cordonService)
	binoculars.RegisterBinocularsServer(grpcServer, binocularsServer)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	grpcCommon.Listen(config.GrpcPort, grpcServer, wg)

	return grpcServer.GracefulStop, wg, nil
}
