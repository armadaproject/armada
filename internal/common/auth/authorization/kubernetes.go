package authorization

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	"io"
	"net/http"
	"net/http/httputil"
	"os"
	"strings"

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
	authHeader := strings.SplitN(metautils.ExtractIncoming(ctx).Get("authorization"), " ", 2)

	if len(authHeader) < 2 || authHeader[0] != "KubernetesAuth" {
		return nil, missingCredentials
	}

	log.Info("Auth extracted")

	token, ca, err := parseAuth(authHeader[1])
	if err != nil {
		return nil, missingCredentials
	}
	// Check Cache

	log.Info("Auth parsed")
	log.Infof("Token: %s", token)
	log.Infof("CA: %s", ca)
	// Get URL from token KID
	url, err := authService.getClusterURL(token)
	if err != nil {
		return nil, err
	}

	log.Info("Token reviewing")
	// Make request to token review endpoint
	name, err := reviewToken(url, token, ca)
	if err != nil {
		return nil, err
	}

	log.Info("Making principle")
	// Return very basic principal
	return NewStaticPrincipal(name, []string{}), nil
}

func (authService *KubernetesNativeAuthService) getClusterURL(token string) (string, error) {
	log.Infof("Token: %s", token)
	header := strings.Split(token, ".")[0]
	log.Infof("Header: %s", header)
	decoded, err := base64.RawURLEncoding.DecodeString(header)
	if err != nil {
		return "", err
	}

	log.Infof("Decoded header: %s", decoded)

	var unmarshalled struct {
		Kid string `json:"kid"`
	}

	if err := json.Unmarshal(decoded, &unmarshalled); err != nil {
		return "", err
	}

	log.Infof("Unmarshalling complete: %v", unmarshalled)
	if unmarshalled.Kid == "" {
		return "", fmt.Errorf("kubernetes serviceaccount token KID must not be empty")
	}

	url, err := os.ReadFile(authService.KidMappingFileLocation + unmarshalled.Kid)
	if err != nil {
		return "", err
	}

	return string(url), nil
}

func reviewToken(clusterUrl string, token string, ca string) (string, error) {
	url := clusterUrl + tokenReviewEndpoint
	data := fmt.Sprintf(`{"kind": "TokenReview","apiVersion": "authentication.k8s.io/v1","spec": {"token": "%s"}}`, token)

	log.Infof("Calling %s for token review with data %s", url, data)
	reviewRequest, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(data)))
	if err != nil {
		return "", err
	}
	reviewRequest.Header.Add("Authorization", "Bearer "+token)
	reviewRequest.Header.Add("Content-Type", "application/json; charset=utf-8")

	reqDump, _ := httputil.DumpRequest(reviewRequest, true)
	log.Infof("Request to send to token review: %s", string(reqDump))

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM([]byte(ca))

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs: caCertPool,
		},
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

func parseAuth(auth string) (string, string, error) {
	jsonData, err := base64.RawURLEncoding.DecodeString(auth)
	log.Printf("Decoded JSON data: %s", jsonData)
	if err != nil {
		return "", "", err
	}

	var uMbody struct {
		Token string `json:"token"`
		Ca    string `json:"ca"`
	}

	if err := json.Unmarshal(jsonData, &uMbody); err != nil {
		log.Infof("Failed to unmarshall %v", err)
		return "", "", err
	}

	ca, err := base64.RawURLEncoding.DecodeString(uMbody.Ca)
	if err != nil {
		return "", "", err
	}

	return uMbody.Token, string(ca), nil
}
