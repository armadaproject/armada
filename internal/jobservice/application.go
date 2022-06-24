package jobservice

import (
	"sync"

	"github.com/G-Research/armada/internal/common/auth/authorization"
	grpcCommon "github.com/G-Research/armada/internal/common/grpc"
	"github.com/G-Research/armada/internal/jobservice/configuration"
	"github.com/G-Research/armada/internal/jobservice/server"
	"github.com/G-Research/armada/pkg/api/jobservice"

	log "github.com/sirupsen/logrus"
)

func StartUp(config *configuration.JobServiceConfiguration) (func(), *sync.WaitGroup) {
	log.Info("Armada jobService service starting")

	wg := sync.WaitGroup{}
	wg.Add(1)

	grpcServer := grpcCommon.CreateGrpcServer(config.Grpc.KeepaliveParams, config.Grpc.KeepaliveEnforcementPolicy, []authorization.AuthService{&authorization.AnonymousAuthService{}})

	jobCacheServer := server.NewJobCacheServer()
	jobservice.RegisterJobServiceServer(grpcServer, jobCacheServer)

	log.Info("JobCache service listening on ", config.GrpcPort)
	grpcCommon.Listen(config.GrpcPort, grpcServer, &wg)

	wg.Wait()
	stop := func() {
		grpcServer.GracefulStop()
	}

	return stop, &wg
}
