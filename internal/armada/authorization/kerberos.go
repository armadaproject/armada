package authorization

import (
	"context"
	"encoding/base64"

	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"gopkg.in/jcmturner/goidentity.v3"
	"gopkg.in/jcmturner/gokrb5.v7/credentials"
	"gopkg.in/jcmturner/gokrb5.v7/gssapi"
	"gopkg.in/jcmturner/gokrb5.v7/keytab"
	"gopkg.in/jcmturner/gokrb5.v7/service"
	"gopkg.in/jcmturner/gokrb5.v7/spnego"
	"gopkg.in/jcmturner/gokrb5.v7/types"

	"github.com/G-Research/armada/internal/armada/configuration"
)

const (
	// spnegoNegTokenRespKRBAcceptCompleted - The response on successful authentication always has this header. Capturing as const so we don't have marshaling and encoding overhead.
	spnegoNegTokenRespKRBAcceptCompleted = "Negotiate oRQwEqADCgEAoQsGCSqGSIb3EgECAg=="
	// spnegoNegTokenRespReject - The response on a failed authentication always has this rejection header. Capturing as const so we don't have marshaling and encoding overhead.
	spnegoNegTokenRespReject = "Negotiate oQcwBaADCgEC"
	// spnegoNegTokenRespIncompleteKRB5 - Response token specifying incomplete context and KRB5 as the supported mechtype.
	spnegoNegTokenRespIncompleteKRB5 = "Negotiate oRQwEqADCgEBoQsGCSqGSIb3EgECAg=="
)

type KerberosAuthService struct {
	kt             *keytab.Keytab
	userNameSuffix string
	settings       []func(*service.Settings)
}

func NewKerberosAuthService(config *configuration.KerberosAuthenticationConfig) (*KerberosAuthService, error) {
	kt, err := keytab.Load(config.KeytabLocation)
	if err != nil {
		return nil, err
	}

	settings := []func(*service.Settings){}
	if config.PrincipalName != "" {
		settings = append(settings, service.KeytabPrincipal(config.PrincipalName))
	}

	return &KerberosAuthService{
		kt:             kt,
		userNameSuffix: config.UserNameSuffix,
		settings:       settings,
	}, nil
}

func (authService *KerberosAuthService) Authenticate(ctx context.Context) (Principal, error) {
	encodedToken, err := grpc_auth.AuthFromMD(ctx, spnego.HTTPHeaderAuthResponseValueKey)
	if err != nil {
		// Add WWW-Authenticate header
		_ = grpc.SetHeader(ctx, metadata.Pairs(spnego.HTTPHeaderAuthResponse, spnego.HTTPHeaderAuthResponseValueKey))
		return nil, missingCredentials
	}

	tokenData, err := base64.StdEncoding.DecodeString(encodedToken)
	if err != nil {
		log.Error("SPNEGO invalid token")
		return nil, status.Errorf(codes.Unauthenticated, "SPNEGO invalid token")
	}

	var token spnego.SPNEGOToken
	err = token.Unmarshal(tokenData)
	if err != nil {
		log.Error("SPNEGO invalid token")
		return nil, status.Errorf(codes.Unauthenticated, "SPNEGO invalid token")
	}

	settings := authService.settings
	p, ok := peer.FromContext(ctx)
	if ok {
		clientHost, e := types.GetHostAddress(p.Addr.String())
		if e == nil {
			settings = append([]func(*service.Settings){service.ClientAddress(clientHost)}, settings...)
		}
	}
	svc := spnego.SPNEGOService(authService.kt, settings...)

	authenticated, ctx, st := svc.AcceptSecContext(&token)
	if st.Code != gssapi.StatusComplete && st.Code != gssapi.StatusContinueNeeded {
		log.Errorf("SPNEGO validation error: %v", st)
		return nil, status.Errorf(codes.Unauthenticated, "SPNEGO validation error: %v", st)
	}
	if st.Code == gssapi.StatusContinueNeeded {
		_ = grpc.SetHeader(ctx, metadata.Pairs(spnego.HTTPHeaderAuthResponse, spnegoNegTokenRespIncompleteKRB5))
		log.Error("SPNEGO GSS-API continue needed")
		return nil, status.Errorf(codes.Unauthenticated, "SPNEGO GSS-API continue needed")
	}
	if authenticated {
		id := ctx.Value(spnego.CTXKeyCredentials).(goidentity.Identity)
		if adCredentials, ok := id.Attributes()[credentials.AttributeKeyADCredentials].(credentials.ADCredentials); ok {
			user := adCredentials.EffectiveName + authService.userNameSuffix
			groups := adCredentials.GroupMembershipSIDs

			log.WithField("user", user).WithField("groups", groups).Info("SPNGO: Logged in!")
			_ = grpc.SetHeader(ctx, metadata.Pairs(spnego.HTTPHeaderAuthResponse, spnegoNegTokenRespKRBAcceptCompleted))
			return NewStaticPrincipal(user, groups), nil
		}
		log.Error("Failed to read ad credentials")
		return nil, status.Errorf(codes.Unauthenticated, "Failed to read ad credentials")

	} else {
		log.Error("SPNEGO Kerberos authentication failed")
		_ = grpc.SetHeader(ctx, metadata.Pairs(spnego.HTTPHeaderAuthResponse, spnegoNegTokenRespReject))
		return nil, status.Errorf(codes.Unauthenticated, "SPNEGO Kerberos authentication failed")
	}
}
