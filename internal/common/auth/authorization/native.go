package authorization

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	grpcAuth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	tokenReviewEndpoint = "/apis/authentication.k8s.io/v1/tokenreviews"
)

// TODO: Should include a cache
type KubernetesNativeAuthService struct {
	KidMappingFileLocation string
}

func (authService *KubernetesNativeAuthService) Authenticate(ctx context.Context) (Principal, error) {
	// Retrieve token from context.
	token, err := grpcAuth.AuthFromMD(ctx, "bearer")
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "valid token was not provided with request")
	}
	// Check Cache

	// Get URL from token KID
	url, err := authService.getClusterURL(token)
	if err != nil {
		return nil, err
	}

	// Make request to token review endpoint
	name, err := reviewToken(url, token)

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

	if err := json.Unmarshal(decoded, unmarshalled); err != nil {
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
	var url = clusterUrl + tokenReviewEndpoint
	var data = fmt.Sprintf(`
{
  "kind": "TokenReview",
  "apiVersion": "authentication.k8s.io/v1"
  "spec": {"token": "%s"}
}`, token)

	log.Infof("Calling %s for token review", url)
	reviewRequest, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(data)))
	if err != nil {
		return "", err
	}
	reviewRequest.Header.Add("Authorization", "Bearer"+token)
	reviewRequest.Header.Add("Content-Type", "application/json; charset=utf-8")

	client := &http.Client{}
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
