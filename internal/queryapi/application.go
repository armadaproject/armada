package queryapi

import (
	"fmt"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"net"

	"github.com/armadaproject/armada/internal/common/app"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/auth"
	"github.com/armadaproject/armada/internal/common/database"
	grpcCommon "github.com/armadaproject/armada/internal/common/grpc"
	"github.com/armadaproject/armada/internal/queryapi/server"
	"github.com/armadaproject/armada/pkg/queryapi"
)

func Run(config Configuration) error {
	g, _ := armadacontext.ErrGroup(app.CreateContextWithShutdown())
	authServices, err := auth.ConfigureAuth(config.Auth)
	if err != nil {
		return errors.WithMessage(err, "error creating auth services")
	}

	db, err := database.OpenPgxPool(config.Postgres)
	if err != nil {
		return errors.WithMessage(err, "error creating postgres pool")
	}
	grpcServer := grpcCommon.CreateGrpcServer(config.Grpc.KeepaliveParams, config.Grpc.KeepaliveEnforcementPolicy, authServices, config.Grpc.Tls)
	defer grpcServer.GracefulStop()
	queryapi.RegisterQueryApiServer(grpcServer, server.New(db))
	log.Infof("QueryApi grpc server listening on %d", config.Grpc.Port)
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Grpc.Port))
	if err != nil {
		return errors.WithStack(err)
	}
	g.Go(func() error {
		return grpcServer.Serve(lis)
	})
	return g.Wait()
}
