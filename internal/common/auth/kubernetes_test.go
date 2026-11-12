package auth

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	grpc_metadata "github.com/grpc-ecosystem/go-grpc-middleware/v2/metadata"
	"github.com/patrickmn/go-cache"
	"github.com/stretchr/testify/assert"
	authv1 "k8s.io/api/authentication/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clock "k8s.io/utils/clock/testing"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/auth/configuration"
)

// A token
const (
	testToken = "eyJhbGciOiJSUzI1NiIsImtpZCI6IkRwWnhkblBHNm1oak1kNjNLU0VJc0cwMWctXzQ1YlU2aXFYeGNraG1xc2MifQ" +
		".eyJhdWQiOlsiaHR0cHM6Ly9rdWJlcm5ldGVzLmRlZmF1bHQuc3ZjLmNsdXN0ZXIubG9jYWwiXSwiZXhwIjoxNjYzMDY4NjYxL" +
		"CJpYXQiOjE2NjMwNjUwNjEsImlzcyI6Imh0dHBzOi8va3ViZXJuZXRlcy5kZWZhdWx0LnN2Yy5jbHVzdGVyLmxvY2FsIiwia3V" +
		"iZXJuZXRlcy5pbyI6eyJuYW1lc3BhY2UiOiJkZWZhdWx0Iiwic2VydmljZWFjY291bnQiOnsibmFtZSI6ImFkbWluLXVzZXIiL" +
		"CJ1aWQiOiIxMGVhZDI5OC1hMTU2LTRkM2QtOTUxMC1jMzNmNGI0NjU5MmQifX0sIm5iZiI6MTY2MzA2NTA2MSwic3ViIjoic3l" +
		"zdGVtOnNlcnZpY2VhY2NvdW50OmRlZmF1bHQ6YWRtaW4tdXNlciJ9.wcRLGXMXd2Ek6JQypyuvijw3pWObwQ1OIfwUMccFe4E0" +
		"8r6_DcmizemfYs2uYuwHovcgqsOqJR3OoMhZd3jo1Irl4XEoeEjqb1qioMl7zpXBcr7kohBUxYbJyWa_FehKsysbB80mw_SG8F" +
		"P3o-ZHo7wL8oE1X0WNoSnOz14ke7MR2ZK3v8E4YfbYVLxilk3E3bnVR3QtyVgo7vn-Y_AHKeZdjHD-sXE6Fb8jO48vvj54JwDW" +
		"-w7qDRYYLgalRU3DXfsil072h_PYMFwtE57NGRjRGyOR2HIZuCVADjt_bF8E87i1TG3ELMCtSE5Jr78lmJ2zhyBFrA_FhljJoKTZJg"
	testTokenIss   = 1663065061
	testCA         = ""
	testTokenExp   = 1663068661
	testKid        = "DpZxdnPG6mhjMd63KSEIsG01g-_45bU6iqXxckhmqsc"
	testUrl        = "https://kubernetes.config.test:420"
	testName       = "admin-user"
	testTokenNoExp = "eyJhbGciOiJSUzI1NiIsImtpZCI6IkRwWnhkblBHNm1oak1kNjNLU0VJc0cwMWctXzQ1YlU2aXFYeGNraG1xc2MifQ" +
		".ewogICJhdWQiOiBbCiAgICAiaHR0cHM6Ly9rdWJlcm5ldGVzLmRlZmF1bHQuc3ZjLmNsdXN0ZXIubG9jYWwiCiAgXSwKICAia" +
		"WF0IjogMTY2MzA2NTA2MSwKICAiaXNzIjogImh0dHBzOi8va3ViZXJuZXRlcy5kZWZhdWx0LnN2Yy5jbHVzdGVyLmxvY2FsIiw" +
		"KICAia3ViZXJuZXRlcy5pbyI6IHsKICAgICJuYW1lc3BhY2UiOiAiZGVmYXVsdCIsCiAgICAic2VydmljZWFjY291bnQiOiB7C" +
		"iAgICAgICJuYW1lIjogImFkbWluLXVzZXIiLAogICAgICAidWlkIjogIjEwZWFkMjk4LWExNTYtNGQzZC05NTEwLWMzM2Y0YjQ" +
		"2NTkyZCIKICAgIH0KICB9LAogICJuYmYiOiAxNjYzMDY1MDYxLAogICJzdWIiOiAic3lzdGVtOnNlcnZpY2VhY2NvdW50OmRlZ" +
		"mF1bHQ6YWRtaW4tdXNlciIKfQ."
)

func TestValidateKid(t *testing.T) {
	assert.Error(t, validateKid(""))
	assert.Error(t, validateKid("../a/dangerous/filepath"))
	assert.NoError(t, validateKid(testKid))
}

func TestParseTime(t *testing.T) {
	myTime, err := parseTime(testToken)
	assert.NoError(t, err)
	assert.Equal(t, time.Unix(testTokenExp, 0), myTime)
}

func TestParseTime_FailsWhenNoExpiry(t *testing.T) {
	myTime, err := parseTime(testTokenNoExp)
	assert.Error(t, err)
	assert.Equal(t, time.Time{}, myTime)
}

func TestGetClusterURL(t *testing.T) {
	// Setup environment
	tempdir, err := os.MkdirTemp("", "kid-mapping")
	defer os.Remove(tempdir)
	if err != nil {
		t.Errorf("TestGetClusterURL returned error: %s", err)
	}
	path := filepath.Join(tempdir, testKid)
	kidfile, err := os.Create(path)
	if err != nil {
		t.Errorf("TestGetClusterURL returned error: %s", err)
	}
	defer os.Remove(path)
	defer kidfile.Close()
	_, err = kidfile.Write([]byte(testUrl))
	if err != nil {
		t.Errorf("KidFile Write Error: %s", err)
	}

	testAuthService := NewKubernetesNativeAuthService(configuration.KubernetesAuthConfig{
		KidMappingFileLocation: tempdir + "/",
	})

	url, err := testAuthService.getClusterURL(testToken)
	if err != nil {
		t.Errorf("TestGetClusterURL returned error: %s", err)
	}

	assert.Equal(t, testUrl, url)
}

type MockTokenReviewer struct {
	Authenticated bool
	Username      string
	// Err, if set, is returned by ReviewToken, as when the Kubernetes API is unavailable.
	Err error
}

func (reviewer *MockTokenReviewer) ReviewToken(ctx context.Context, clusterUrl string, token string, ca []byte) (*authv1.TokenReview, error) {
	if reviewer.Err != nil {
		return nil, reviewer.Err
	}
	return &authv1.TokenReview{
		Status: authv1.TokenReviewStatus{
			Authenticated: reviewer.Authenticated,
			User: authv1.UserInfo{
				Username: reviewer.Username,
			},
		},
	}, nil
}

func createTestAuthService(kidMapping string, authenticated bool, username string, currentTime int64) KubernetesNativeAuthService {
	cache := cache.New(5*time.Minute, 5*time.Minute)
	return KubernetesNativeAuthService{
		KidMappingFileLocation: kidMapping,
		TokenCache:             cache,
		InvalidTokenExpiry:     6000,
		TokenReviewer: &MockTokenReviewer{
			Authenticated: authenticated,
			Username:      username,
		},
		Clock: clock.NewFakeClock(time.Unix(currentTime, 0)),
	}
}

func createKubernetesAuthPayload(token string, ca string) string {
	encodedCa := base64.RawURLEncoding.EncodeToString([]byte(ca))
	body := fmt.Sprintf(`{"token":"%s", "ca":"%s"}`, token, encodedCa)
	return "KubernetesAuth " + base64.RawURLEncoding.EncodeToString([]byte(body))
}

func TestAuthenticateKubernetes(t *testing.T) {
	// Setup KID mapping directory
	tempdir, err := os.MkdirTemp("", "kid-mapping")
	defer os.Remove(tempdir)
	if err != nil {
		t.Errorf("TestGetClusterURL returned error: %s", err)
	}
	path := filepath.Join(tempdir, testKid)
	kidfile, err := os.Create(path)
	if err != nil {
		t.Errorf("TestGetClusterURL returned error: %s", err)
	}
	defer os.Remove(path)
	defer kidfile.Close()
	_, err = kidfile.Write([]byte(testUrl))
	if err != nil {
		t.Errorf("KidFile Write returned error: %s", err)
	}

	// Create authentication context
	payload := createKubernetesAuthPayload(testToken, testCA)
	ctx := context.Background()
	metadata := grpc_metadata.ExtractIncoming(ctx)
	metadata.Set("authorization", payload)
	ctx = metadata.ToIncoming(ctx)

	// Authenticate
	authService := createTestAuthService(tempdir+"/", true, testName, testTokenIss)
	principal, err := authService.Authenticate(ctx, payload)

	expected := NewStaticPrincipal(testName, KubernetesAuthServiceName, []string{testName})
	assert.NoError(t, err)
	assert.Equal(t, expected, principal)
}

// When the Kubernetes API cannot review the token, the credentials have not been found wanting, so the error is an
// internal one and not that the credentials are invalid.
func TestAuthenticateKubernetes_ReviewFailureIsAnInternalError(t *testing.T) {
	tempdir := t.TempDir()
	assert.NoError(t, os.WriteFile(filepath.Join(tempdir, testKid), []byte(testUrl), 0o600))

	authService := createTestAuthService(tempdir+"/", true, testName, testTokenIss)
	authService.TokenReviewer = &MockTokenReviewer{Err: errors.New("kubernetes api unavailable")}

	principal, err := authService.Authenticate(context.Background(), createKubernetesAuthPayload(testToken, testCA))

	assert.Nil(t, principal)
	var internalErr *armadaerrors.ErrInternalAuthServiceError
	assert.ErrorAs(t, err, &internalErr)
	var invalidErr *armadaerrors.ErrInvalidCredentials
	assert.False(t, errors.As(err, &invalidErr), "an outage is not invalid credentials")
	_, cached := authService.TokenCache.Get(testToken)
	assert.False(t, cached, "an outage must not be remembered as a verdict on the token")
}

// A token that the Kubernetes API reviews and rejects is still invalid credentials.
func TestAuthenticateKubernetes_RejectedTokenIsInvalidCredentials(t *testing.T) {
	tempdir := t.TempDir()
	assert.NoError(t, os.WriteFile(filepath.Join(tempdir, testKid), []byte(testUrl), 0o600))

	authService := createTestAuthService(tempdir+"/", false, "", testTokenIss)

	_, err := authService.Authenticate(context.Background(), createKubernetesAuthPayload(testToken, testCA))

	var invalidErr *armadaerrors.ErrInvalidCredentials
	assert.ErrorAs(t, err, &invalidErr)
}

// The token is what the review is made with, so the Kubernetes API refusing it with a 401 is a token that has been
// revoked or has expired, and is invalid credentials. Other failures of the API stay internal errors.
func TestAuthenticateKubernetes_TokenTheApiRefusesIsInvalidCredentials(t *testing.T) {
	tempdir := t.TempDir()
	assert.NoError(t, os.WriteFile(filepath.Join(tempdir, testKid), []byte(testUrl), 0o600))

	authService := createTestAuthService(tempdir+"/", true, testName, testTokenIss)
	authService.TokenReviewer = &MockTokenReviewer{Err: apierrors.NewUnauthorized("Unauthorized")}

	principal, err := authService.Authenticate(context.Background(), createKubernetesAuthPayload(testToken, testCA))

	assert.Nil(t, principal)
	var invalidErr *armadaerrors.ErrInvalidCredentials
	assert.ErrorAs(t, err, &invalidErr)
	var internalErr *armadaerrors.ErrInternalAuthServiceError
	assert.False(t, errors.As(err, &internalErr), "a refused token is not an outage")
}

func TestAuthenticateKubernetes_ApiFailuresOtherThanUnauthorizedAreInternalErrors(t *testing.T) {
	for name, reviewErr := range map[string]error{
		"unavailable":    apierrors.NewServiceUnavailable("etcd is down"),
		"server timeout": apierrors.NewServerTimeout(schema.GroupResource{}, "create", 1),
		"internal error": apierrors.NewInternalError(errors.New("boom")),
		"not a status":   errors.New("connection refused"),
	} {
		t.Run(name, func(t *testing.T) {
			tempdir := t.TempDir()
			assert.NoError(t, os.WriteFile(filepath.Join(tempdir, testKid), []byte(testUrl), 0o600))
			authService := createTestAuthService(tempdir+"/", true, testName, testTokenIss)
			authService.TokenReviewer = &MockTokenReviewer{Err: reviewErr}

			_, err := authService.Authenticate(context.Background(), createKubernetesAuthPayload(testToken, testCA))

			var internalErr *armadaerrors.ErrInternalAuthServiceError
			assert.ErrorAs(t, err, &internalErr)
		})
	}
}
