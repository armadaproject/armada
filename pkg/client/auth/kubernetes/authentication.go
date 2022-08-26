package kubernetes

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	log "github.com/sirupsen/logrus"

	"golang.org/x/oauth2"

	"github.com/G-Research/armada/pkg/client/auth/oidc"
)

const (
	tokenRequestEndpoint = "/api/v1/namespaces/%s/serviceaccounts/%s/token"
)

type NativeAuthDetails struct {
	Expiry         int64
	APIUrl         string
	Namespace      string
	ServiceAccount string
}

func AuthenticateKubernetesNative(config NativeAuthDetails) (*NativeTokenCredentials, error) {
	localKubernetesToken, err := getKubernetesToken() // Only need to get the token once
	if err != nil {
		return nil, err
	}

	tokenSource := oidc.FunctionTokenSource{
		GetToken: func() (*oauth2.Token, error) {
			log.Info("Getting new Kubernetes token from TokenRequest")
			url := config.APIUrl + fmt.Sprintf(tokenRequestEndpoint, config.Namespace, config.ServiceAccount)
			data := fmt.Sprintf(`
{
	"apiVersion": "authentication.k8s.io/v1",
	"kind": "TokenRequest",
	"spec": {
		"expirationSeconds": %d
	}
}`, config.Expiry)

			// Make HTTPs Request to TokenRequest endpoint
			request, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(data)))
			if err != nil {
				return nil, err
			}
			request.Header.Add("Authorization", "Bearer "+localKubernetesToken)
			request.Header.Add("Content-Type", "application/json; charset=utf-8")

			client := &http.Client{}
			resp, err := client.Do(request)
			if err != nil {
				return nil, fmt.Errorf("error getting temporary token: %v", err)
			}

			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				return nil, err
			}
			// The JWT we receive is not technically a JWT token, however we wish to leverage
			// the library to perform caching for us.
			return parseToken(body)
		},
	}

	return &NativeTokenCredentials{TokenSource: oauth2.ReuseTokenSource(nil, &tokenSource)}, nil
}

// parseToken reads the TokenRequest response body and puts it into the form of an oauth2 token
func parseToken(body []byte) (*oauth2.Token, error) {
	var unmarshalledToken responseBody
	if err := json.Unmarshal(body, &unmarshalledToken); err != nil {
		return nil, fmt.Errorf("kubernetes kubernetes auth: cannot unmarshal token: %v", err)
	}

	return &oauth2.Token{
		AccessToken: unmarshalledToken.Status.Token,
		Expiry:      unmarshalledToken.Status.Expiration,
	}, nil
}

type responseBody struct {
	Status responseStatus `json:"status"`
}

type responseStatus struct {
	Token      string    `json:"token"`
	Expiration time.Time `json:"expirationTimestamp"`
}

func getKubernetesToken() (string, error) {
	fromEnv := os.Getenv("K8S_SERVICEACCOUNT_TOKEN")
	if fromEnv != "" {
		return fromEnv, nil
	}

	fromFile, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
	if err != nil {
		return "", err
	}

	return string(fromFile), nil
}
