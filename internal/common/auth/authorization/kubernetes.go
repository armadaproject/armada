package authorization

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	"github.com/patrickmn/go-cache"
	authv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/auth/configuration"
)

type TokenReviewer interface {
	ReviewToken(ctx context.Context, clusterUrl string, token string, ca []byte) (*authv1.TokenReview, error)
}

type KubernetesTokenReviewer struct{}

func (reviewer *KubernetesTokenReviewer) ReviewToken(ctx context.Context, clusterUrl string, token string, ca []byte) (*authv1.TokenReview, error) {
	config := &rest.Config{
		Host:            clusterUrl,
		BearerToken:     token,
		TLSClientConfig: rest.TLSClientConfig{CAData: ca},
	}
	clientSet, err := kubernetes.NewForConfig(config)
	if err != nil {
		return &authv1.TokenReview{}, err
	}

	tr := authv1.TokenReview{
		Spec: authv1.TokenReviewSpec{
			Token: token,
		},
	}

	return clientSet.AuthenticationV1().TokenReviews().Create(ctx, &tr, metav1.CreateOptions{})
}

type KubernetesNativeAuthService struct {
	KidMappingFileLocation string
	TokenCache             *cache.Cache
	InvalidTokenExpiry     int64
	TokenReviewer          TokenReviewer
	Clock                  clock.Clock
}

func NewKubernetesNativeAuthService(config configuration.KubernetesAuthConfig) KubernetesNativeAuthService {
	cache := cache.New(5*time.Minute, 5*time.Minute)
	return KubernetesNativeAuthService{
		KidMappingFileLocation: config.KidMappingFileLocation,
		TokenCache:             cache,
		InvalidTokenExpiry:     config.InvalidTokenExpiry,
		TokenReviewer:          &KubernetesTokenReviewer{},
		Clock:                  clock.RealClock{},
	}
}

type CacheData struct {
	Name  string `json:"name"`
	Valid bool   `json:"valid"`
}

func (authService *KubernetesNativeAuthService) Name() string {
	return "KubernetesNative"
}

func (authService *KubernetesNativeAuthService) Authenticate(ctx context.Context) (Principal, error) {
	// Retrieve token from context.
	authHeader := strings.SplitN(metautils.ExtractIncoming(ctx).Get("authorization"), " ", 2)

	if len(authHeader) < 2 || authHeader[0] != "KubernetesAuth" {
		return nil, &armadaerrors.ErrMissingCredentials{
			AuthService: authService.Name(),
		}
	}

	token, ca, err := parseAuth(authHeader[1])
	if err != nil {
		return nil, &armadaerrors.ErrInvalidCredentials{
			AuthService: authService.Name(),
		}
	}

	// Get token time
	expirationTime, err := parseTime(token)
	if err != nil {
		return nil, &armadaerrors.ErrInvalidCredentials{
			AuthService: authService.Name(),
			Message:     err.Error(),
		}
	}

	if authService.Clock.Now().After(expirationTime) {
		return nil, &armadaerrors.ErrInvalidCredentials{
			AuthService: authService.Name(),
			Message:     "invalid token, expired",
		}
	}

	// Check Cache
	data, found := authService.TokenCache.Get(token)
	if found {
		if cacheInfo, ok := data.(CacheData); ok {
			if cacheInfo.Valid {
				return NewStaticPrincipal(cacheInfo.Name, []string{cacheInfo.Name}), nil
			} else {
				return nil, &armadaerrors.ErrInvalidCredentials{
					AuthService: authService.Name(),
					Message:     "token invalid",
				}
			}
		}
	}

	// Get URL from token KID
	url, err := authService.getClusterURL(token)
	if err != nil {
		return nil, &armadaerrors.ErrInvalidCredentials{
			AuthService: authService.Name(),
			Message:     err.Error(),
		}
	}

	// Make request to token review endpoint
	name, err := authService.reviewToken(ctx, url, token, []byte(ca))
	if err != nil {
		// reviewToken returns appropriate armadaerrors.
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

	// Return very basic Principal
	return NewStaticPrincipal(name, []string{name}), nil
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

	if err = validateKid(unmarshalled.Kid); err != nil {
		return "", err
	}

	url, err := os.ReadFile(authService.KidMappingFileLocation + unmarshalled.Kid)
	if err != nil {
		return "", err
	}

	return string(url), nil
}

func (authService *KubernetesNativeAuthService) reviewToken(ctx context.Context, clusterUrl string, token string, ca []byte) (string, error) {
	result, err := authService.TokenReviewer.ReviewToken(ctx, clusterUrl, token, ca)
	if err != nil {
		// TODO(clif) Hard to tell if this should be internal auth error
		// or invalid creds still.
		return "", &armadaerrors.ErrInternalAuthServiceError{
			AuthService: authService.Name(),
			Message:     err.Error(),
		}
	}

	if !result.Status.Authenticated {
		authService.TokenCache.Set(token, CacheData{Valid: false}, time.Duration(authService.InvalidTokenExpiry))
		return "", &armadaerrors.ErrInvalidCredentials{
			AuthService: authService.Name(),
			Message:     "provided token was rejected by TokenReview",
		}
	}

	return result.Status.User.Username, nil
}

func parseAuth(auth string) (string, string, error) {
	jsonData, err := base64.RawURLEncoding.DecodeString(auth)
	if err != nil {
		return "", "", err
	}

	var uMbody struct {
		Token string `json:"token"`
		Ca    string `json:"ca"`
	}

	if err := json.Unmarshal(jsonData, &uMbody); err != nil {
		return "", "", err
	}

	ca, err := base64.RawURLEncoding.DecodeString(uMbody.Ca)
	if err != nil {
		return "", "", err
	}

	return uMbody.Token, string(ca), nil
}

func parseTime(token string) (time.Time, error) {
	splitToken := strings.Split(token, ".")
	if len(splitToken) != 3 {
		return time.Time{}, fmt.Errorf("provided JWT token was not of the correct form, should have 3 parts")
	}

	decoded, err := base64.RawURLEncoding.DecodeString(splitToken[1])
	if err != nil {
		return time.Time{}, err
	}
	var uMbody struct {
		Expiry int64 `json:"exp"`
	}

	if err := json.Unmarshal(decoded, &uMbody); err != nil {
		return time.Time{}, err
	}

	if uMbody.Expiry == 0 {
		return time.Time{}, fmt.Errorf("token expiry time not set")
	}

	time := time.Unix(uMbody.Expiry, 0)
	return time, nil
}

func validateKid(kid string) error {
	if kid == "" {
		return fmt.Errorf("kubernetes serviceaccount token KID must not be empty")
	}

	if strings.Contains(kid, "../") {
		return fmt.Errorf("kid appears to contain ../, this appears to be an attack")
	}

	return nil
}
