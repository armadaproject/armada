package kubernetes

import (
	"context"

	log "github.com/sirupsen/logrus"
	"golang.org/x/oauth2"
	authv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/G-Research/armada/pkg/client/auth/oidc"
)

type NativeAuthDetails struct {
	Expiry         int64
	Namespace      string
	ServiceAccount string
}

func AuthenticateKubernetesNative(config NativeAuthDetails) (*NativeTokenCredentials, error) {
	tokenSource := oidc.FunctionTokenSource{
		GetToken: func() (*oauth2.Token, error) {
			log.Infof("Getting new temporary token from TokenReview")
			kubeConfig, err := rest.InClusterConfig()
			if err != nil {
				return nil, err
			}
			clientSet, err := kubernetes.NewForConfig(kubeConfig)
			tr := authv1.TokenRequest{
				Spec: authv1.TokenRequestSpec{
					ExpirationSeconds: &config.Expiry,
				},
			}

			result, err := clientSet.CoreV1().ServiceAccounts(config.Namespace).
				CreateToken(context.Background(), config.ServiceAccount, &tr, metav1.CreateOptions{})
			if err != nil {
				return nil, err
			}

			return &oauth2.Token{
				AccessToken: result.Status.Token,
				Expiry:      result.Status.ExpirationTimestamp.Time,
			}, nil
		},
	}

	return &NativeTokenCredentials{TokenSource: oauth2.ReuseTokenSource(nil, &tokenSource)}, nil
}
