package authorization

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/coreos/go-oidc"
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

	ctx := metadata.NewIncomingContext(context.Background(), map[string][]string{
		"authorization": {"bearer " + token},
	})

	service := NewOpenIdAuthService(verifier, "groups")

	principal, e := service.Authenticate(ctx)
	assert.Nil(t, e)
	assert.Equal(t, "me", principal.GetName())
	assert.True(t, principal.IsInGroup("test"))

	_, e = service.Authenticate(context.Background())
	var missingCredsErr *armadaerrors.ErrMissingCredentials
	assert.ErrorAs(t, e, &missingCredsErr)

	keySet.err = errors.New("wrong signature")
	_, e = service.Authenticate(ctx)
	assert.NotNil(t, e)
	assert.NotErrorIs(t, e, missingCredsErr)
}

type fakeKeySet struct {
	payload []byte
	err     error
}

func (k *fakeKeySet) VerifySignature(ctx context.Context, jwt string) (payload []byte, err error) {
	return k.payload, k.err
}
