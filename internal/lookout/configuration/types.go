package configuration

import (
	"time"

	profilingconfig "github.com/armadaproject/armada/internal/common/profiling/configuration"
	"github.com/armadaproject/armada/internal/server/configuration"
)

type LookoutConfig struct {
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
	Name string
	// Tempate is the template string for the command
	Template string
	// DescriptionMd is an optional description for the command in Markdown
	DescriptionMd string
	// AlertMessageMd is an optional message for the command, to be displayed as
	// an alert, written in Markdown
	AlertMessageMd string
	// AlertLevel is the severity level of the alert
	AlertLevel AlertLevel
}

type UIConfig struct {
	CustomTitle string

	// We have a separate flag here (instead of making the Oidc field optional)
	// so that clients can override the server's preference.
	OidcEnabled bool
	Oidc        struct {
		Authority string
		ClientId  string
		Scope     string
	}

	ArmadaApiBaseUrl         string
	UserAnnotationPrefix     string
	BinocularsBaseUrlPattern string

	JobSetsAutoRefreshMs int `json:",omitempty"`
	JobsAutoRefreshMs    int `json:",omitempty"`
	CommandSpecs         []CommandSpec

	Backend string `json:",omitempty"`
}
