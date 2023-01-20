package configuration

import (
	"time"

	"github.com/armadaproject/armada/internal/common/auth/permission"
)

type AuthConfig struct {
	AnonymousAuth bool

	BasicAuth      BasicAuthenticationConfig
	KubernetesAuth KubernetesAuthConfig
	OpenIdAuth     OpenIdAuthenticationConfig
	Kerberos       KerberosAuthenticationConfig

	PermissionGroupMapping map[permission.Permission][]string
	PermissionScopeMapping map[permission.Permission][]string
	PermissionClaimMapping map[permission.Permission][]string
}

type UserInfo struct {
	Password string
	Groups   []string
}

type OpenIdAuthenticationConfig struct {
	ProviderUrl string
	GroupsClaim string

	// If your OIDC provider signs token with key intended solely for this application and audience claim does not
	// contain any clientId, you can disable client ID check.
	// Otherwise clientId is required
	SkipClientIDCheck bool
	ClientId          string
}

type BasicAuthenticationConfig struct {
	Users map[string]UserInfo
}

type KerberosAuthenticationConfig struct {
	KeytabLocation  string
	PrincipalName   string
	UserNameSuffix  string
	GroupNameSuffix string
	LDAP            LDAPConfig
}

type LDAPConfig struct {
	URL             string
	Username        string
	Password        string
	GroupSearchBase string
	CacheExpiry     time.Duration
}

type KubernetesAuthConfig struct {
	KidMappingFileLocation string
	InvalidTokenExpiry     int64
}
