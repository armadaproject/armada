package kerberos

import (
	"context"
	"encoding/base64"

	"github.com/alexbrainman/sspi/negotiate"
	"github.com/prometheus/common/log"
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

	securityCtx, token, err := negotiate.NewClientContext(cred, "HTTP/armada.armada.services-1.ospr-k8s-d.eqld.c3.zone")
	if err != nil {
		return nil, err
	}
	defer securityCtx.Release()

	log.Info(base64.StdEncoding.EncodeToString(token))

	return negotiateHeader(token), nil
}

func (s spnego) RequireTransportSecurity() bool {
	return true
}
