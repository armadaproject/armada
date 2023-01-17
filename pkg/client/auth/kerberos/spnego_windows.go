package kerberos

import (
	"context"

	"github.com/alexbrainman/sspi/negotiate"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/pkg/errors"
	"google.golang.org/grpc/credentials"

	"github.com/armadaproject/armada/internal/common/logging"
)

type spnego struct {
	spn string
}

func NewSPNEGOCredentials(serverUrl string, config ClientConfig) (credentials.PerRPCCredentials, error) {
	spn, e := urlToSpn(serverUrl)
	if e != nil {
		return nil, e
	}
	return &spnego{spn: spn}, nil
}

func (s *spnego) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	// TODO: keep clientContext for multiple requests
	log := ctxlogrus.Extract(ctx)

	cred, err := negotiate.AcquireCurrentUserCredentials()
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := cred.Release(); err != nil {
			err = errors.WithStack(err)
			logging.WithStacktrace(log, err).Error("failed to release cred")
		}
	}()

	securityCtx, token, err := negotiate.NewClientContext(cred, s.spn)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := securityCtx.Release(); err != nil {
			err = errors.WithStack(err)
			logging.WithStacktrace(log, err).Error("failed to release security context")
		}
	}()

	return negotiateHeader(token), nil
}

func (s spnego) RequireTransportSecurity() bool {
	return true
}
