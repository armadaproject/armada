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

type LookoutThemeConfigOptions struct {
	// Typography
	FontFamily            *string `json:"fontFamily,omitempty"`
	MonospaceFontFamily   *string `json:"monospaceFontFamily,omitempty"`
	UppercaseButtonText   *bool   `json:"uppercaseButtonText,omitempty"`
	UppercaseOverlineText *bool   `json:"uppercaseOverlineText,omitempty"`

	// Shape
	BorderRadiusPx *int `json:"borderRadiusPx,omitempty"`

	// Palette (light, default)
	PrimaryColour                *string `json:"primaryColour,omitempty"`
	SecondaryColour              *string `json:"secondaryColour,omitempty"`
	AppBarColour                 *string `json:"appBarColour,omitempty"`
	ErrorColour                  *string `json:"errorColour,omitempty"`
	WarningColour                *string `json:"warningColour,omitempty"`
	InfoColour                   *string `json:"infoColour,omitempty"`
	SuccessColour                *string `json:"successColour,omitempty"`
	StatusQueuedColour           *string `json:"statusQueuedColour,omitempty"`
	StatusPendingColour          *string `json:"statusPendingColour,omitempty"`
	StatusRunningColour          *string `json:"statusRunningColour,omitempty"`
	StatusSucceededColour        *string `json:"statusSucceededColour,omitempty"`
	StatusFailedColour           *string `json:"statusFailedColour,omitempty"`
	StatusCancelledColour        *string `json:"statusCancelledColour,omitempty"`
	StatusPreemptedColour        *string `json:"statusPreemptedColour,omitempty"`
	StatusLeasedColour           *string `json:"statusLeasedColour,omitempty"`
	StatusRejectedColour         *string `json:"statusRejectedColour,omitempty"`
	DefaultBackgroundColour      *string `json:"defaultBackgroundColour,omitempty"`
	PaperSurfaceBackgroundColour *string `json:"paperSurfaceBackgroundColour,omitempty"`

	// Palette (dark)
	PrimaryColourDark                *string `json:"primaryColourDark,omitempty"`
	SecondaryColourDark              *string `json:"secondaryColourDark,omitempty"`
	AppBarColourDark                 *string `json:"appBarColourDark,omitempty"`
	ErrorColourDark                  *string `json:"errorColourDark,omitempty"`
	WarningColourDark                *string `json:"warningColourDark,omitempty"`
	InfoColourDark                   *string `json:"infoColourDark,omitempty"`
	SuccessColourDark                *string `json:"successColourDark,omitempty"`
	StatusQueuedColourDark           *string `json:"statusQueuedColourDark,omitempty"`
	StatusPendingColourDark          *string `json:"statusPendingColourDark,omitempty"`
	StatusRunningColourDark          *string `json:"statusRunningColourDark,omitempty"`
	StatusSucceededColourDark        *string `json:"statusSucceededColourDark,omitempty"`
	StatusFailedColourDark           *string `json:"statusFailedColourDark,omitempty"`
	StatusCancelledColourDark        *string `json:"statusCancelledColourDark,omitempty"`
	StatusPreemptedColourDark        *string `json:"statusPreemptedColourDark,omitempty"`
	StatusLeasedColourDark           *string `json:"statusLeasedColourDark,omitempty"`
	StatusRejectedColourDark         *string `json:"statusRejectedColourDark,omitempty"`
	DefaultBackgroundColourDark      *string `json:"defaultBackgroundColourDark,omitempty"`
	PaperSurfaceBackgroundColourDark *string `json:"paperSurfaceBackgroundColourDark,omitempty"`
}
type CustomTheme struct {
	Name        string                    `json:"name"`
	ThemeConfig LookoutThemeConfigOptions `json:"themeConfig"`
}

type CustomThemeConfigs struct {
	Themes           []CustomTheme `json:"themes"`
	DefaultThemeName string        `json:"defaultThemeName"`
}

type JobLinkConfig struct {
	// Label is the text which is displayed in the chip for this link
	Label string `json:"label"`
	// Colour is the colour of the chip for this link
	Colour string `json:"colour"`
	// LinkTemplate is the template string for the link. For example, use 'annotations["your.anno/tation-key"]' to interpolate the value of an annotation
	LinkTemplate string `json:"linkTemplate"`
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

	JobLinks []JobLinkConfig `json:"jobLinks"`

	CustomThemeConfigs *CustomThemeConfigs `json:"customThemeConfigs,omitempty"`
}
