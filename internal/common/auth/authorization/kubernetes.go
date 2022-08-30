package authorization

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	grpcAuth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/common/auth/configuration"
)

const (
	tokenReviewEndpoint = "/apis/authentication.k8s.io/v1/tokenreviews"
)

// TODO: Should include a cache
type KubernetesNativeAuthService struct {
	KidMappingFileLocation string
}

func NewKubernetesNativeAuthService(config configuration.KubernetesAuthConfig) KubernetesNativeAuthService {
	return KubernetesNativeAuthService{
		KidMappingFileLocation: config.KidMappingFileLocation,
	}
}

func (authService *KubernetesNativeAuthService) Authenticate(ctx context.Context) (Principal, error) {
	// Retrieve token from context.
	token, err := grpcAuth.AuthFromMD(ctx, "kubernetesAuth")
	if err != nil {
		return nil, missingCredentials
	}
	// Check Cache

	// Get URL from token KID
	url, err := authService.getClusterURL(token)
	if err != nil {
		return nil, err
	}

	// Make request to token review endpoint
	name, err := reviewToken(url, token)
	if err != nil {
		return nil, err
	}

	// Return very basic principal
	return NewStaticPrincipal(name, []string{}), nil
}

func (authService *KubernetesNativeAuthService) getClusterURL(token string) (string, error) {
	header := strings.Split(token, ".")[0]
	decoded, err := base64.RawURLEncoding.DecodeString(header)
	if err != nil {
		return "", err
	}

	var unmarshalled struct {
		Kid string `json:"kid"`
	}

	if err := json.Unmarshal(decoded, &unmarshalled); err != nil {
		return "", err
	}

	if unmarshalled.Kid == "" {
		return "", fmt.Errorf("kubernetes serviceaccount token KID must not be empty")
	}

	url, err := os.ReadFile(authService.KidMappingFileLocation + unmarshalled.Kid)
	if err != nil {
		return "", err
	}

	return string(url), nil
}

func reviewToken(clusterUrl string, token string) (string, error) {
	url := clusterUrl + tokenReviewEndpoint
	data := fmt.Sprintf(`
{
  "kind": "TokenReview",
  "apiVersion": "authentication.k8s.io/v1"
  "spec": {"token": "%s"}
}`, token)

	log.Infof("Calling %s for token review with data %s", url, data)
	reviewRequest, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(data)))
	if err != nil {
		return "", err
	}
	reviewRequest.Header.Add("Authorization", "Bearer"+token)
	reviewRequest.Header.Add("Content-Type", "application/json; charset=utf-8")

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	client := &http.Client{Transport: tr}

	resp, err := client.Do(reviewRequest)
	if err != nil {
		return "", err
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	status, err := parseReviewResponse(body)
	if err != nil {
		return "", err
	}
	if !status.Authenticated {
		return "", fmt.Errorf("provided token was rejected by TokenReview")
	}

	return status.User.Username, nil
}

func parseReviewResponse(body []byte) (*reviewStatus, error) {
	var uMbody reviewBody
	if err := json.Unmarshal(body, &uMbody); err != nil {
		return nil, err
	}

	return &uMbody.Status, nil
}

type reviewUser struct {
	Username string `json:"username"`
}

type reviewStatus struct {
	Authenticated bool       `json:"authenticated"`
	User          reviewUser `json:"user"`
}

type reviewBody struct {
	Status reviewStatus `json:"status"`
}
