package kerberos

import (
	"context"

	"github.com/alexbrainman/sspi/negotiate"
	"google.golang.org/grpc/credentials"
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

	cred, err := negotiate.AcquireCurrentUserCredentials()
	if err != nil {
		return nil, err
	}
	defer cred.Release()

	securityCtx, token, err := negotiate.NewClientContext(cred, s.spn)
	if err != nil {
		return nil, err
	}
	defer securityCtx.Release()

	return negotiateHeader(token), nil
}

func (s spnego) RequireTransportSecurity() bool {
	return true
}
