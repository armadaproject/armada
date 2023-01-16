package kubernetes

import (
	"context"
	"os"

	log "github.com/sirupsen/logrus"
	"golang.org/x/oauth2"
	authv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/armadaproject/armada/pkg/client/auth/oidc"
)

type NativeAuthDetails struct {
	Expiry  int64
	Enabled bool
}

func AuthenticateKubernetesNative(config NativeAuthDetails) (*NativeTokenCredentials, error) {
	serviceAccount := os.Getenv("SERVICE_ACCOUNT")
	namespace := os.Getenv("POD_NAMESPACE")

	tokenSource := oidc.FunctionTokenSource{
		GetToken: func() (*oauth2.Token, error) {
			log.Infof("Getting new temporary token from TokenReview")
			// TODO: Possibly remove kubeConfig and clientSet from inside this function?
			kubeConfig, err := rest.InClusterConfig()
			if err != nil {
				return nil, err
			}
			clientSet, err := kubernetes.NewForConfig(kubeConfig)
			if err != nil {
				return nil, err
			}
			tr := authv1.TokenRequest{
				Spec: authv1.TokenRequestSpec{
					ExpirationSeconds: &config.Expiry,
				},
			}

			result, err := clientSet.CoreV1().ServiceAccounts(namespace).
				CreateToken(context.Background(), serviceAccount, &tr, metav1.CreateOptions{})
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
