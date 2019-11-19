// +build linux darwin

package kerberos

import (
	"context"

	"google.golang.org/grpc/credentials"
)

func NewSPNEGOCredentials(serverUrl string, config ClientConfig) (credentials.PerRPCCredentials, error) {
	return &spnego{}, nil
}

type spnego struct{}

func (s spnego) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	panic("SPNEGO Kerberos auth is implemented just for windows.")
}
func (s spnego) RequireTransportSecurity() bool {
	return true
}
