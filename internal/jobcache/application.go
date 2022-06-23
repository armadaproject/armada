package jobcache

import (
	"sync"

	"github.com/G-Research/armada/internal/common/auth/authorization"
	grpcCommon "github.com/G-Research/armada/internal/common/grpc"
	"github.com/G-Research/armada/internal/jobcache/configuration"
	"github.com/G-Research/armada/internal/jobcache/server"
	"github.com/G-Research/armada/pkg/api/jobcache"

	log "github.com/sirupsen/logrus"
)

func StartUp(config *configuration.JobCacheConfiguration) (func(), *sync.WaitGroup) {
	log.Info("Armada JobCache service starting")

	wg := sync.WaitGroup{}
	wg.Add(1)

	grpcServer := grpcCommon.CreateGrpcServer(config.Grpc.KeepaliveParams, config.Grpc.KeepaliveEnforcementPolicy, []authorization.AuthService{&authorization.AnonymousAuthService{}})

	jobCacheServer := server.NewJobCacheServer()
	jobcache.RegisterJobCacheServer(grpcServer, jobCacheServer)

	log.Info("JobCache service listening on ", config.GrpcPort)
	grpcCommon.Listen(config.GrpcPort, grpcServer, &wg)

	wg.Wait()
	log.Info("I got here")
	stop := func() {
		grpcServer.GracefulStop()
	}

	return stop, &wg
}
