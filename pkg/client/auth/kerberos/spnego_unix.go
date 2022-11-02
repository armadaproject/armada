//go:build linux || darwin
// +build linux darwin

package kerberos

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/user"
	"sync"
	"time"

	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/credentials"
	"github.com/jcmturner/gokrb5/v8/krberror"
	"github.com/jcmturner/gokrb5/v8/spnego"
	grpc_credentials "google.golang.org/grpc/credentials"
)

type spnegoCredentials struct {
	spn                      string
	krb5Config               *config.Config
	credentialsCachePath     string
	credentialsFileRefreshed time.Time

	kerberosClient *client.Client
	mux            sync.Mutex
}

func NewSPNEGOCredentials(serverUrl string, spnegoConfig ClientConfig) (grpc_credentials.PerRPCCredentials, error) {
	spn, e := urlToSpn(serverUrl)
	if e != nil {
		return nil, e
	}

	configPath := os.Getenv("KRB5_CONFIG")
	if configPath == "" {
		configPath = "/etc/krb5.conf"
	}
	krb5Config, err := config.Load(configPath)
	if err != nil {
		return nil, err
	}

	credentialsCachePath := os.Getenv("KRB5CCNAME")
	if credentialsCachePath == "" {
		currentUser, err := user.Current()
		if err != nil {
			return nil, err
		}
		credentialsCachePath = "/tmp/krb5cc_" + currentUser.Uid
	}

	s := &spnegoCredentials{
		spn:                  spn,
		krb5Config:           krb5Config,
		credentialsCachePath: credentialsCachePath,
	}

	err = s.renewClient()
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *spnegoCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	err := s.renewClient()
	if err != nil {
		return nil, fmt.Errorf("could not renew client: %v", err)
	}
	spnegoClient := spnego.SPNEGOClient(s.kerberosClient, s.spn)
	err = spnegoClient.AcquireCred()
	if err != nil {
		return nil, fmt.Errorf("could not acquire client credential: %v. This is often resolved by performing a kinit and trying again.", err)
	}
	st, err := spnegoClient.InitSecContext()
	if err != nil {
		return nil, fmt.Errorf("could not initialise context: %v", err)
	}
	token, err := st.Marshal()
	if err != nil {
		return nil, krberror.Errorf(err, krberror.EncodingError, "could not marshal SPNEGO token")
	}
	return negotiateHeader(token), nil
}

func (s *spnegoCredentials) renewClient() error {
	s.mux.Lock()
	defer s.mux.Unlock()

	now := time.Now()

	if s.kerberosClient == nil ||
		s.kerberosClient.Credentials == nil ||
		s.kerberosClient.Credentials.Expired() ||
		s.credentialsFileRefreshed.Add(5*time.Minute).Before(now) {

		credentialsCache, err := credentials.LoadCCache(s.credentialsCachePath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("unable to find your kerberos credential cache at %s, this is often resolved by performing a kinit and trying again.",
					s.credentialsCachePath)
			}
			return fmt.Errorf("failed to load your kerberos cred cache from %s: %s",
				s.credentialsCachePath, err)
		}
		kerberosClient, err := client.NewFromCCache(credentialsCache, s.krb5Config, client.DisablePAFXFAST(true))
		if err != nil {
			return err
		}
		s.kerberosClient = kerberosClient
		s.credentialsFileRefreshed = now
	}

	return nil
}

func (s spnegoCredentials) RequireTransportSecurity() bool {
	return true
}
