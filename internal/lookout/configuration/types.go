package configuration

import (
	"time"

	authconfig "github.com/armadaproject/armada/internal/common/auth/configuration"
	profilingconfig "github.com/armadaproject/armada/internal/common/profiling/configuration"
	"github.com/armadaproject/armada/internal/server/configuration"
)

type LookoutConfig struct {
	Auth authconfig.AuthConfig

	ApiPort     int
	Profiling   *profilingconfig.ProfilingConfig
	MetricsPort int

	CorsAllowedOrigins []string
	Tls                TlsConfig

	Postgres configuration.PostgresConfig

	PrunerConfig PrunerConfig

	UIConfig
}

type TlsConfig struct {
	Enabled  bool
	KeyPath  string
	CertPath string
}

type PrunerConfig struct {
	ExpireAfter              time.Duration
	DeduplicationExpireAfter time.Duration
	Timeout                  time.Duration
	BatchSize                int
	Postgres                 configuration.PostgresConfig
}

// Alert level enum values correspond to the severity levels of the MUI Alert
// component: https://mui.com/material-ui/react-alert/#severity
type AlertLevel string

const (
	AlertLevelSuccess AlertLevel = "success"
	AlertLevelInfo    AlertLevel = "info"
	AlertLevelWarning AlertLevel = "warning"
	AlertLevelError   AlertLevel = "error"
)

// CommandSpec details a command to be displayed on a job's "Commands" sidebar
// tab in the Lookout UI
type CommandSpec struct {
	// Name is the title of the command
	Name string `json:"name"`
	// Template is the template string for the command
	Template string `json:"template"`
	// DescriptionMd is an optional description for the command in Markdown
	DescriptionMd *string `json:"descriptionMd,omitempty"`
	// AlertMessageMd is an optional message for the command, to be displayed as
	// an alert, written in Markdown
	AlertMessageMd *string `json:"alertMessageMd,omitempty"`
	// AlertLevel is the severity level of the alert
	AlertLevel AlertLevel `json:"alertLevel"`
}

type SentryConfig struct {
	Dsn         string `json:"dsn"`
	Environment string `json:"environment"`
}

type ErrorMonitoringConfig struct {
	Sentry *SentryConfig `json:"sentry,omitempty"`
}

// UIConfig must match the LookoutUiConfig TypeScript interface defined in internal/lookoutui/src/lookoutUiConfig.d.ts
type UIConfig struct {
	CustomTitle string `json:"customTitle"`

	// We have a separate flag here (instead of making the Oidc field optional)
	// so that clients can override the server's preference.
	OidcEnabled bool `json:"oidcEnabled"`
	Oidc        *struct {
		Authority string `json:"authority"`
		ClientId  string `json:"clientId"`
		Scope     string `json:"scope"`
	} `json:"oidc,omitempty"`

	ArmadaApiBaseUrl         string `json:"armadaApiBaseUrl"`
	UserAnnotationPrefix     string `json:"userAnnotationPrefix"`
	BinocularsBaseUrlPattern string `json:"binocularsBaseUrlPattern"`

	JobSetsAutoRefreshMs int           `json:"jobSetsAutoRefreshMs,omitempty"`
	JobsAutoRefreshMs    int           `json:"jobsAutoRefreshMs,omitempty"`
	CommandSpecs         []CommandSpec `json:"commandSpecs"`

	Backend string `json:"backend,omitempty"`

	// PinnedTimeZoneIdentifiers is the list of identifiers of IANA time zones to be displayed at
	// the top of the time zone selector.
	PinnedTimeZoneIdentifiers []string `json:"pinnedTimeZoneIdentifiers"`

	ErrorMonitoring ErrorMonitoringConfig `json:"errorMonitoring"`
}
