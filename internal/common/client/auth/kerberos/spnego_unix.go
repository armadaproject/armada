// +build linux darwin

package kerberos

import (
	"context"
	"fmt"
	"os"
	"os/user"
	"sync"
	"time"

	grpc_credentials "google.golang.org/grpc/credentials"
	"gopkg.in/jcmturner/gokrb5.v7/client"
	"gopkg.in/jcmturner/gokrb5.v7/config"
	"gopkg.in/jcmturner/gokrb5.v7/credentials"
	"gopkg.in/jcmturner/gokrb5.v7/krberror"
	"gopkg.in/jcmturner/gokrb5.v7/spnego"
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

	currentUser, err := user.Current()
	if err != nil {
		return nil, err
	}
	credentialsCachePath := os.Getenv("KRB5CCNAME")
	if credentialsCachePath == "" {
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
		return nil, fmt.Errorf("could not acquire client credential: %v", err)
	}
	st, err := spnegoClient.InitSecContext()
	if err != nil {
		return nil, fmt.Errorf("could not initialize context: %v", err)
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
			return err
		}
		kerberosClient, err := client.NewClientFromCCache(credentialsCache, s.krb5Config, client.DisablePAFXFAST(true))
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
