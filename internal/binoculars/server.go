package binoculars

import (
	"context"
	"os"
	"sync"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/binoculars/configuration"
	"github.com/armadaproject/armada/internal/binoculars/server"
	"github.com/armadaproject/armada/internal/binoculars/service"
	"github.com/armadaproject/armada/internal/common/auth"
	"github.com/armadaproject/armada/internal/common/auth/authorization"
	"github.com/armadaproject/armada/internal/common/cluster"
	"github.com/armadaproject/armada/internal/common/fileutils"
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

	authServices, err := auth.ConfigureAuth(config.Auth)
	if err != nil {
		log.Errorf("Failed to create auth services %s", err)
		os.Exit(-1)
	}

	var cachedCertificateService *fileutils.CachedCertificateService
	if config.Grpc.Tls.Enabled {
		cachedCertificateService = fileutils.NewCachedCertificateService(config.Grpc.Tls.CertPath, config.Grpc.Tls.KeyPath)
		go func() {
			err := func() error {
				return cachedCertificateService.Run(context.Background())
			}()
			if err != nil {
				log.WithError(err).Errorf("failed refreshing certificate")
			}
		}()
	}
	grpcServer := grpcCommon.CreateGrpcServer(config.Grpc.KeepaliveParams, config.Grpc.KeepaliveEnforcementPolicy, authServices, cachedCertificateService)

	permissionsChecker := authorization.NewPrincipalPermissionChecker(
		config.Auth.PermissionGroupMapping,
		config.Auth.PermissionScopeMapping,
		config.Auth.PermissionClaimMapping,
	)

	logService := service.NewKubernetesLogService(kubernetesClientProvider)
	cordonService := service.NewKubernetesCordonService(config.Cordon, permissionsChecker, kubernetesClientProvider)
	binocularsServer := server.NewBinocularsServer(logService, cordonService)
	binoculars.RegisterBinocularsServer(grpcServer, binocularsServer)
	grpc_prometheus.Register(grpcServer)

	grpcCommon.Listen(config.GrpcPort, grpcServer, wg)

	return grpcServer.GracefulStop, wg
}
