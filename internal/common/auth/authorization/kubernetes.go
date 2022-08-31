package authorization

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	"github.com/patrickmn/go-cache"
	log "github.com/sirupsen/logrus"
	authv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/G-Research/armada/internal/common/auth/configuration"
)

// TODO: Should include a cache
type KubernetesNativeAuthService struct {
	KidMappingFileLocation string
	TokenCache             *cache.Cache
}

func NewKubernetesNativeAuthService(config configuration.KubernetesAuthConfig) KubernetesNativeAuthService {
	cache := cache.New(5*time.Minute, 5*time.Minute)
	return KubernetesNativeAuthService{
		KidMappingFileLocation: config.KidMappingFileLocation,
		TokenCache:             cache,
	}
}

type CacheData struct {
	Name  string `json:"name"`
	Valid bool   `json:"valid"`
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

	log.Info("Auth parsed")

	// Get token time
	expirationTime, err := parseTime(token)
	if err != nil {
		return nil, err
	}

	if time.Now().After(*expirationTime) {
		return nil, fmt.Errorf("invalid token, expired")
	}

	// Check Cache
	data, found := authService.TokenCache.Get(token)
	if found {
		if cacheInfo, ok := data.(CacheData); ok && cacheInfo.Valid {
			return NewStaticPrincipal(cacheInfo.Name, []string{}), nil
		}
	}

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
	name, err := reviewToken(ctx, url, token, []byte(ca))
	if err != nil {
		return nil, err
	}

	// Add to cache
	authService.TokenCache.Set(
		token,
		CacheData{
			Name:  name,
			Valid: true,
		},
		expirationTime.Sub(time.Now()))
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

func reviewToken(ctx context.Context, clusterUrl string, token string, ca []byte) (string, error) {
	config := &rest.Config{
		Host:            clusterUrl,
		BearerToken:     token,
		TLSClientConfig: rest.TLSClientConfig{CAData: ca},
	}
	clientSet, err := kubernetes.NewForConfig(config)
	if err != nil {
		return "", err
	}

	tr := authv1.TokenReview{
		Spec: authv1.TokenReviewSpec{
			Token: token,
		},
	}

	result, err := clientSet.AuthenticationV1().TokenReviews().Create(ctx, &tr, metav1.CreateOptions{})
	if err != nil {
		return "", err
	}

	if err != nil {
		return "", err
	}
	if !result.Status.Authenticated {
		return "", fmt.Errorf("provided token was rejected by TokenReview")
	}

	return result.Status.User.Username, nil
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

func parseTime(token string) (*time.Time, error) {
	splitToken := strings.Split(token, ".")
	if len(splitToken) != 3 {
		return nil, fmt.Errorf("provided JWT token was not of the correct form, should have 3 parts")
	}

	decoded, err := base64.RawURLEncoding.DecodeString(splitToken[1])
	if err != nil {
		return nil, err
	}

	var uMbody struct {
		Expiry int64 `json:"exp"`
	}

	if err := json.Unmarshal(decoded, &uMbody); err != nil {
		log.Infof("Failed to unmarshall to get time information: %v", err)
		return nil, err
	}

	time := time.Unix(uMbody.Expiry, 0)
	return &time, nil
}
