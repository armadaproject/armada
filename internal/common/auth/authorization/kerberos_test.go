package authorization

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	"github.com/jcmturner/gokrb5/v8/credentials"
	"github.com/jcmturner/gokrb5/v8/gssapi"
	"github.com/jcmturner/gokrb5/v8/iana/etypeID"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/jcmturner/gokrb5/v8/service"
	"github.com/jcmturner/gokrb5/v8/spnego"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/auth/configuration"
)

const (
	testUser       = "testUser"
	testPass       = "testPass"
	testRealm      = "testRealm"
	testGSSAPIInit = "608202b606062b0601050502a08202aa308202a6a027302506092a" +
		"864886f71201020206052b0501050206092a864882f71201020206062b060105020" +
		"5a2820279048202756082027106092a864886f71201020201006e8202603082025c" +
		"a003020105a10302010ea20703050000000000a38201706182016c30820168a0030" +
		"20105a10d1b0b544553542e474f4b524235a2233021a003020103a11a30181b0448" +
		"5454501b10686f73742e746573742e676f6b726235a382012b30820127a00302011" +
		"2a103020102a282011904820115d4bd890abc456f44e2e7a2e8111bd6767abf0326" +
		"6dfcda97c629af2ece450a5ae1f145e4a4d1bc2c848e66a6c6b31d9740b26b03cdb" +
		"d2570bfcf126e90adf5f5ebce9e283ff5086da47b129b14fc0aabd4d1df9c1f3c72" +
		"b80cc614dfc28783450b2c7b7749651f432b47aaa2ff158c0066b757f3fb00dd7b4" +
		"f63d68276c76373ecdd3f19c66ebc43a81e577f3c263b878356f57e8d6c4eccd587" +
		"b81538e70392cf7e73fc12a6f7c537a894a7bb5566c83ac4d69757aa320a51d8d69" +
		"0017aebf952add1889adfc3307b0e6cd8c9b57cf8589fbe52800acb6461c25473d4" +
		"9faa1bdceb8bce3f61db23f9cd6a09d5adceb411e1c4546b30b33331e570fd6bc50" +
		"aa403557e75f488e759750ea038aab6454667d9b64f41a481d23081cfa003020112" +
		"a281c70481c4eb593beb5afcb1a2a669d54cb85a3772231559f2d40c9f8f053f218" +
		"ba6eb084ed7efc467d94b88bcd189dda920d6e675ec001a6a2bca11f0a1de37f2f7" +
		"ae9929f94a86d625b2ec1b213a88cbae6099dda7b172cd3bd1802cb177ae4554d59" +
		"277004bfd3435248f55044fe7af7b2c9c5a3c43763278c585395aebe2856cdff9f2" +
		"569d8b823564ce6be2d19748b910ec06bd3c0a9bc5de51ddcf7d875f1108ca6ad93" +
		"5f52d90cb62a18197d9b8e796bef0fbe1463f61df61cfbce6008ae9e1a2d2314a986d"
)

// Create a keytab and generate a key and save it to a temp file.
func generateTempKeytab() (string, error) {
	f, err := os.CreateTemp("", "test_keytab")
	if err != nil {
		return "", err
	}
	defer func() {
		if err := f.Close(); err != nil {
			panic(err)
		}
	}()

	// Create a single entry.
	kt := keytab.New()
	err = kt.AddEntry(testUser, testRealm, testPass,
		time.Now(), 1, etypeID.AES128_CTS_HMAC_SHA256_128)
	if err != nil {
		return "", err
	}
	// Write to temp file and close it.
	_, err = kt.Write(f)
	if err != nil {
		return "", err
	}

	return f.Name(), nil
}

type mockGroupLookup struct{}

func (mgl *mockGroupLookup) GetGroupNames(SIDs []string) ([]string, error) {
	// For now, just return one group name for all inputs.
	return []string{"testGroup"}, nil
}

func setupNewTestKerberosAuthService(keytabFilename string) (*KerberosAuthService, error) {
	kAuthCfg := &configuration.KerberosAuthenticationConfig{
		KeytabLocation:  keytabFilename,
		PrincipalName:   testUser,
		UserNameSuffix:  "testUsernameSuffix",
		GroupNameSuffix: "testGroupnameSuffix",
		LDAP:            configuration.LDAPConfig{}, // Unused, LDAP should not be called.
	}

	kAuthSvc, err := NewKerberosAuthService(kAuthCfg, &mockGroupLookup{})
	if err != nil {
		return nil, err
	}

	kAuthSvc.newSpnegoSvc = func(kt *keytab.Keytab, options ...func(*service.Settings)) SPNEGOService {
		return &mockSPNEGOService{kt: kt}
	}

	return kAuthSvc, nil
}

type mockSPNEGOService struct {
	kt *keytab.Keytab

	// Values returned by AcceptSecContext
	authed bool
	ctx    context.Context
	status gssapi.Status
}

func (mss *mockSPNEGOService) AcceptSecContext(token gssapi.ContextToken) (bool, context.Context, gssapi.Status) {
	return mss.authed, mss.ctx, mss.status
}

func getContextWithEncodedKerberosToken(rawToken []byte) context.Context {
	nMD := metautils.NiceMD{}
	nMD.Set(
		"authorization",
		spnego.HTTPHeaderAuthResponseValueKey+" "+base64.StdEncoding.EncodeToString(rawToken))
	return nMD.ToIncoming(context.Background())
}

func TestKerberosAuthenticateMissingCreds(t *testing.T) {
	WithTestKerberosAuthService(func(kAuthSvc *KerberosAuthService) {
		assert.NotNil(t, kAuthSvc)

		// Nothing on the context, should result in ErrMissingCredentials
		principal, err := kAuthSvc.Authenticate(context.Background())
		require.Nil(t, principal)
		missingCredsErr := &armadaerrors.ErrMissingCredentials{}
		assert.ErrorAs(t, err, &missingCredsErr)
	})
}

func TestKerberosAuthenticateBadHeaderToken(t *testing.T) {
	WithTestKerberosAuthService(func(kAuthSvc *KerberosAuthService) {
		// Improperly formatted token in header
		nMD := metautils.NiceMD{}
		nMD.Set(
			"authorization",
			spnego.HTTPHeaderAuthResponseValueKey)

		principal, err := kAuthSvc.Authenticate(nMD.ToIncoming(context.Background()))
		assert.Nil(t, principal)
		missingCredsErr := &armadaerrors.ErrMissingCredentials{}
		assert.ErrorAs(t, err, &missingCredsErr)
	})
}

func TestKerberosAuthenticateUnmarshalTokenError(t *testing.T) {
	WithTestKerberosAuthService(func(kAuthSvc *KerberosAuthService) {
		// Unmarshal error
		principal, err := kAuthSvc.Authenticate(
			getContextWithEncodedKerberosToken([]byte("")))
		assert.Nil(t, principal)
		invalidCredsErr := &armadaerrors.ErrInvalidCredentials{}
		assert.ErrorAs(t, err, &invalidCredsErr)
	})
}

func TestKerberosAuthenticateSPNEGOValidationError(t *testing.T) {
	WithTestKerberosAuthService(func(kAuthSvc *KerberosAuthService) {
		// SPNEGO validation error
		kAuthSvc.newSpnegoSvc = func(kt *keytab.Keytab, options ...func(*service.Settings)) SPNEGOService {
			return &mockSPNEGOService{
				kt:     kAuthSvc.kt,
				authed: false,
				ctx:    context.Background(),
				status: gssapi.Status{
					Code:    gssapi.StatusDefectiveToken,
					Message: "Defective Token",
				},
			}
		}
		token, err := hex.DecodeString(testGSSAPIInit)
		assert.Nil(t, err)
		principal, err := kAuthSvc.Authenticate(
			getContextWithEncodedKerberosToken(token))
		assert.Nil(t, principal)
		invalidCredsErr := &armadaerrors.ErrInvalidCredentials{}
		assert.ErrorAs(t, err, &invalidCredsErr)
	})
}

func TestKerberosAuthenticateStatusContinueError(t *testing.T) {
	WithTestKerberosAuthService(func(kAuthSvc *KerberosAuthService) {
		// Status continue error
		kAuthSvc.newSpnegoSvc = func(kt *keytab.Keytab, options ...func(*service.Settings)) SPNEGOService {
			return &mockSPNEGOService{
				kt:     kAuthSvc.kt,
				authed: false,
				ctx:    context.Background(),
				status: gssapi.Status{
					Code:    gssapi.StatusContinueNeeded,
					Message: "Continue needed",
				},
			}
		}
		token, err := hex.DecodeString(testGSSAPIInit)
		assert.Nil(t, err)
		principal, err := kAuthSvc.Authenticate(
			getContextWithEncodedKerberosToken(token))
		assert.Nil(t, principal)
		invalidCredsErr := &armadaerrors.ErrInvalidCredentials{}
		assert.ErrorAs(t, err, &invalidCredsErr)
	})
}

func TestKerberosAuthenticateFailedToReadADCreds(t *testing.T) {
	WithTestKerberosAuthService(func(kAuthSvc *KerberosAuthService) {
		// Failed to read ad creds error
		creds := credentials.New(testUser, testRealm)
		credContext := context.Background()
		credContext = context.WithValue(credContext, ctxCredentials, creds)

		kAuthSvc.newSpnegoSvc = func(kt *keytab.Keytab, options ...func(*service.Settings)) SPNEGOService {
			return &mockSPNEGOService{
				kt:     kAuthSvc.kt,
				authed: true,
				ctx:    credContext,
				status: gssapi.Status{
					Code:    gssapi.StatusComplete,
					Message: "Auth success",
				},
			}
		}
		token, err := hex.DecodeString(testGSSAPIInit)
		require.NoError(t, err)
		principal, err := kAuthSvc.Authenticate(
			getContextWithEncodedKerberosToken(token))
		assert.Nil(t, principal)
		invalidCredsErr := &armadaerrors.ErrInvalidCredentials{}
		assert.ErrorAs(t, err, &invalidCredsErr)
	})
}

func TestKerberosAuthenticateSuccess(t *testing.T) {
	WithTestKerberosAuthService(func(kAuthSvc *KerberosAuthService) {
		// Success auth
		creds := credentials.New(testUser, testRealm)

		adCreds := credentials.ADCredentials{
			EffectiveName:       "testEffectiveName",
			GroupMembershipSIDs: []string{"testSID"},
		}
		creds.SetAttribute(credentials.AttributeKeyADCredentials, adCreds)
		credContext := context.Background()
		credContext = context.WithValue(credContext, ctxCredentials, creds)

		kAuthSvc.newSpnegoSvc = func(kt *keytab.Keytab, options ...func(*service.Settings)) SPNEGOService {
			return &mockSPNEGOService{
				kt:     kAuthSvc.kt,
				authed: true,
				ctx:    credContext,
				status: gssapi.Status{
					Code:    gssapi.StatusComplete,
					Message: "Auth success",
				},
			}
		}

		token, err := hex.DecodeString(testGSSAPIInit)
		require.NoError(t, err)
		principal, err := kAuthSvc.Authenticate(
			getContextWithEncodedKerberosToken(token))
		assert.NotNil(t, principal)
		require.NoError(t, err)
	})
}

func TestKerberosAuthenticateAuthFailure(t *testing.T) {
	WithTestKerberosAuthService(func(kAuthSvc *KerberosAuthService) {
		// Failed auth due to invalid creds
		creds := credentials.New(testUser, testRealm)

		adCreds := credentials.ADCredentials{
			EffectiveName:       "testEffectiveName",
			GroupMembershipSIDs: []string{"testSID"},
		}
		creds.SetAttribute(credentials.AttributeKeyADCredentials, adCreds)
		credContext := context.Background()
		credContext = context.WithValue(credContext, ctxCredentials, creds)

		kAuthSvc.newSpnegoSvc = func(kt *keytab.Keytab, options ...func(*service.Settings)) SPNEGOService {
			return &mockSPNEGOService{
				kt:     kAuthSvc.kt,
				authed: false,
				ctx:    credContext,
				status: gssapi.Status{
					Code:    gssapi.StatusComplete,
					Message: "Auth failed",
				},
			}
		}

		token, err := hex.DecodeString(testGSSAPIInit)
		require.NoError(t, err)
		principal, err := kAuthSvc.Authenticate(
			getContextWithEncodedKerberosToken(token))
		assert.Nil(t, principal)
		invalidCredsErr := &armadaerrors.ErrInvalidCredentials{}
		assert.ErrorAs(t, err, &invalidCredsErr)
	})
}

func WithTestKerberosAuthService(action func(*KerberosAuthService)) {
	tempKeytab, err := generateTempKeytab()
	if err != nil {
		panic(err)
	}
	defer os.Remove(tempKeytab)

	kAuthSvc, err := setupNewTestKerberosAuthService(tempKeytab)
	if err != nil {
		panic(err)
	}

	action(kAuthSvc)
}
