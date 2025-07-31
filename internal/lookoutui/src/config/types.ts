export interface SentryConfig {
  dsn: string
  environment: string
}

export interface ErrorMonitoringConfig {
  sentry: SentryConfig | undefined
}

export interface OidcConfig {
  authority: string
  clientId: string
  scope: string
}

export interface CommandSpec {
  name: string
  template: string
  descriptionMd: string | undefined
  alertMessageMd: string | undefined
  alertLevel: string
}

// This must match the UIConfig Go struct defined in internal/lookout/configuration/types.go
export interface LookoutUiConfig {
  armadaApiBaseUrl: string
  userAnnotationPrefix: string
  binocularsBaseUrlPattern: string
  jobSetsAutoRefreshMs: number | undefined
  jobsAutoRefreshMs: number | undefined
  customTitle: string
  oidcEnabled: boolean
  oidc: OidcConfig | undefined
  commandSpecs: CommandSpec[]
  backend: string | undefined
  pinnedTimeZoneIdentifiers: string[]
  errorMonitoring: ErrorMonitoringConfig
}

export interface Config extends LookoutUiConfig {
  debugEnabled: boolean
  fakeDataEnabled: boolean
}

declare global {
  interface Window {
    __LOOKOUT_UI_CONFIG__?: LookoutUiConfig
  }
}
