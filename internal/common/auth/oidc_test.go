package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
)

func TestOpenIdAuthService(t *testing.T) {
	payload, _ := json.Marshal(map[string]interface{}{
		"sub":    "me",
		"iss":    "fake_issuer",
		"exp":    time.Now().Add(time.Hour).Unix(),
		"groups": []string{"test"},
	})
	token := fmt.Sprintf("%s.%s.42",
		base64.RawURLEncoding.EncodeToString([]byte("{\"alg\":\"RS256\"}")),
		base64.RawURLEncoding.EncodeToString(payload))
	keySet := &fakeKeySet{payload, nil}
	verifier := oidc.NewVerifier("fake_issuer", keySet, &oidc.Config{SkipClientIDCheck: true})

	authHeader := "bearer " + token
	ctx := metadata.NewIncomingContext(context.Background(), map[string][]string{
		"authorization": {authHeader},
	})

	service := NewOpenIdAuthService(verifier, "groups")

	principal, e := service.Authenticate(ctx, authHeader)
	assert.Nil(t, e)
	assert.Equal(t, "me", principal.GetName())
	assert.True(t, principal.IsInGroup("test"))

	_, e = service.Authenticate(context.Background(), "")
	var missingCredsErr *armadaerrors.ErrMissingCredentials
	assert.ErrorAs(t, e, &missingCredsErr)

	keySet.err = errors.New("wrong signature")
	_, e = service.Authenticate(ctx, authHeader)
	assert.NotNil(t, e)
	assert.NotErrorIs(t, e, missingCredsErr)
}

// The signing keys are fetched by go-oidc's own RemoteKeySet, so that a change to the error it returns when the fetch
// fails is caught here.
func TestOpenIdAuthService_KeyFetchFailure(t *testing.T) {
	payload, _ := json.Marshal(map[string]interface{}{
		"sub": "me",
		"iss": "fake_issuer",
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	// The key id is not one the verifier has cached, so it has to fetch the provider's keys.
	authHeader := fmt.Sprintf("bearer %s.%s.42",
		base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","kid":"unknown"}`)),
		base64.RawURLEncoding.EncodeToString(payload))

	serviceWithJwks := func(t *testing.T, handler http.HandlerFunc) *OpenIdAuthService {
		jwks := httptest.NewServer(handler)
		t.Cleanup(jwks.Close)
		keySet := oidc.NewRemoteKeySet(context.Background(), jwks.URL)
		verifier := oidc.NewVerifier("fake_issuer", keySet, &oidc.Config{SkipClientIDCheck: true})
		return NewOpenIdAuthService(verifier, "groups")
	}

	t.Run("the provider failing is an internal error", func(t *testing.T) {
		service := serviceWithJwks(t, func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "provider down", http.StatusInternalServerError)
		})
		_, err := service.Authenticate(context.Background(), authHeader)
		var internalErr *armadaerrors.ErrInternalAuthServiceError
		assert.ErrorAs(t, err, &internalErr)
		assert.Equal(t, http.StatusInternalServerError, httpStatusForAuthError(err))
	})

	t.Run("a token the provider's keys do not verify is invalid credentials", func(t *testing.T) {
		service := serviceWithJwks(t, func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"keys":[]}`))
		})
		_, err := service.Authenticate(context.Background(), authHeader)
		var invalidErr *armadaerrors.ErrInvalidCredentials
		assert.ErrorAs(t, err, &invalidErr)
		assert.Equal(t, http.StatusUnauthorized, httpStatusForAuthError(err))
	})
}

type fakeKeySet struct {
	payload []byte
	err     error
}

func (k *fakeKeySet) VerifySignature(ctx context.Context, jwt string) (payload []byte, err error) {
	return k.payload, k.err
}
