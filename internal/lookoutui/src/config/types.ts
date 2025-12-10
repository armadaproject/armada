import { LookoutThemeConfigOptions } from "../theme"

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

export interface CustomTheme {
  name: string
  themeConfig: LookoutThemeConfigOptions
}

export interface CustomThemeConfigs {
  themes: CustomTheme[]
  defaultThemeName: string
}

export interface JobLinkConfig {
  label: string
  colour: string
  linkTemplate: string
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
  jobLinks: JobLinkConfig[]
  backend: string | undefined
  pinnedTimeZoneIdentifiers: string[]
  errorMonitoring: ErrorMonitoringConfig
  customThemeConfigs: CustomThemeConfigs | undefined
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
