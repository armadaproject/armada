package queryapi

import (
	"github.com/pkg/errors"

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
	return g.Wait()
}
