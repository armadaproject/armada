package authorization

import (
	"context"
	"encoding/base64"
	"fmt"

	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"github.com/jcmturner/gokrb5/v8/credentials"
	"github.com/jcmturner/gokrb5/v8/gssapi"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/jcmturner/gokrb5/v8/service"
	"github.com/jcmturner/gokrb5/v8/spnego"
	"github.com/jcmturner/gokrb5/v8/types"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/auth/authorization/groups"
	"github.com/armadaproject/armada/internal/common/auth/configuration"
)

// Partly reimplementing github.com/jcmturner/gokrb5/v8/spnego/http.go for GRPC
// Copying constants as they are private
const (
	// spnegoNegTokenRespReject - The response on a failed authentication always has this rejection header.
	// Capturing as const so we don't have marshalling and encoding overhead.
	spnegoNegTokenRespReject = "Negotiate oQcwBaADCgEC"
	// spnegoNegTokenRespIncompleteKRB5 - Response token specifying incomplete context and KRB5 as the supported mechtype.
	spnegoNegTokenRespIncompleteKRB5 = "Negotiate oRQwEqADCgEBoQsGCSqGSIb3EgECAg=="
	// ctxCredentials is the SPNEGO context key holding the credentials jcmturner/goidentity/Identity object.
	ctxCredentials = "github.com/jcmturner/gokrb5/v8/ctxCredentials"

	SIDAuthenticationAuthorityAssertedIdentity = "S-1-18-1"
)

type KerberosAuthService struct {
	kt              *keytab.Keytab
	userNameSuffix  string
	groupNameSuffix string
	settings        []func(*service.Settings)
	groupLookup     groups.GroupLookup

	// This allows tests to replace the SPNEGO service with a mocked one.
	newSpnegoSvc func(*keytab.Keytab, ...func(*service.Settings)) SPNEGOService
}

func NewKerberosAuthService(config *configuration.KerberosAuthenticationConfig, groupLookup groups.GroupLookup) (*KerberosAuthService, error) {
	kt, err := keytab.Load(config.KeytabLocation)
	if err != nil {
		return nil, err
	}

	settings := []func(*service.Settings){}
	if config.PrincipalName != "" {
		settings = append(settings, service.KeytabPrincipal(config.PrincipalName))
	}

	return &KerberosAuthService{
		kt:              kt,
		userNameSuffix:  config.UserNameSuffix,
		groupNameSuffix: config.GroupNameSuffix,
		settings:        settings,
		groupLookup:     groupLookup,
		newSpnegoSvc: func(kt *keytab.Keytab, options ...func(*service.Settings)) SPNEGOService {
			return spnego.SPNEGOService(kt, options...)
		},
	}, nil
}

func (authService *KerberosAuthService) Name() string {
	return "SPNEGO Kerberos"
}

type SPNEGOService interface {
	AcceptSecContext(gssapi.ContextToken) (bool, context.Context, gssapi.Status)
}

func (authService *KerberosAuthService) Authenticate(ctx context.Context) (Principal, error) {
	encodedToken, err := grpc_auth.AuthFromMD(ctx, spnego.HTTPHeaderAuthResponseValueKey)
	if err != nil {
		// Add WWW-Authenticate header
		_ = grpc.SetHeader(ctx, metadata.Pairs(spnego.HTTPHeaderAuthResponse, spnego.HTTPHeaderAuthResponseValueKey))
		return nil, &armadaerrors.ErrMissingCredentials{
			AuthService: authService.Name(),
			Message:     err.Error(),
		}
	}

	tokenData, err := base64.StdEncoding.DecodeString(encodedToken)
	if err != nil {
		log.Errorf("SPNEGO invalid token, could not decode: %v", err)
		return nil, &armadaerrors.ErrInvalidCredentials{
			Message:     "SPNEGO invalid token, could not decode",
			AuthService: authService.Name(),
		}
	}

	var token spnego.SPNEGOToken
	err = token.Unmarshal(tokenData)
	if err != nil {
		log.Errorf("SPNEGO invalid token, could not unmarshal : %v", err)
		return nil, &armadaerrors.ErrInvalidCredentials{
			Message:     "SPNEGO invalid token, could not unmarshal",
			AuthService: authService.Name(),
		}
	}

	settings := authService.settings
	p, ok := peer.FromContext(ctx)
	if ok {
		clientHost, e := types.GetHostAddress(p.Addr.String())
		if e == nil {
			settings = append([]func(*service.Settings){service.ClientAddress(clientHost)}, settings...)
		}
	}
	svc := authService.newSpnegoSvc(authService.kt, settings...)

	authenticated, credentialsContext, st := svc.AcceptSecContext(&token)
	if st.Code != gssapi.StatusComplete && st.Code != gssapi.StatusContinueNeeded {
		log.Errorf("SPNEGO validation error: %v", st)
		return nil, &armadaerrors.ErrInvalidCredentials{
			Message:     fmt.Sprintf("SPNEGO validation error: %v", st),
			AuthService: authService.Name(),
		}
	}
	if st.Code == gssapi.StatusContinueNeeded {
		_ = grpc.SetHeader(ctx, metadata.Pairs(spnego.HTTPHeaderAuthResponse, spnegoNegTokenRespIncompleteKRB5))
		log.Error("SPNEGO GSS-API continue needed")
		return nil, &armadaerrors.ErrInvalidCredentials{
			Message:     "SPNEGO GSS-API continue needed",
			AuthService: authService.Name(),
		}
	}
	if authenticated {
		id := credentialsContext.Value(ctxCredentials).(*credentials.Credentials)
		if adCredentials, ok := id.Attributes()[credentials.AttributeKeyADCredentials].(credentials.ADCredentials); ok {
			user := adCredentials.EffectiveName + authService.userNameSuffix

			groupSIDs := []string{}
			for _, sid := range adCredentials.GroupMembershipSIDs {
				if sid != SIDAuthenticationAuthorityAssertedIdentity {
					groupSIDs = append(groupSIDs, sid)
				}
			}

			userGroups, err := authService.mapUserGroups(groupSIDs)
			if err != nil {
				return nil, err
			}

			// Original library sets ticket accepted header here, but this breaks python
			// request-negotiate-sspi module
			// removing the header as workaround before moving away from kerberos
			return NewStaticPrincipal(user, userGroups), nil
		}
		log.Error("Failed to read ad credentials")
		return nil, &armadaerrors.ErrInvalidCredentials{
			Message:     "Failed to read ad credentials",
			AuthService: authService.Name(),
		}

	} else {
		log.Error("SPNEGO Kerberos authentication failed")
		_ = grpc.SetHeader(ctx, metadata.Pairs(spnego.HTTPHeaderAuthResponse, spnegoNegTokenRespReject))
		return nil, &armadaerrors.ErrInvalidCredentials{
			AuthService: authService.Name(),
			Message:     "SPNEGO Kerberos authentication failed",
		}
	}
}

func (authService *KerberosAuthService) mapUserGroups(groupSIDs []string) ([]string, error) {
	if authService.groupLookup != nil {
		userGroups, err := authService.groupLookup.GetGroupNames(groupSIDs)
		if err != nil {
			return nil, &armadaerrors.ErrInternalAuthServiceError{
				AuthService: authService.Name(),
				Message:     err.Error(),
			}
		}
		prefixedUserGroups := []string{}
		for _, group := range userGroups {
			prefixedUserGroups = append(prefixedUserGroups, group+authService.groupNameSuffix)
		}
		return prefixedUserGroups, nil
	}
	return groupSIDs, nil
}
