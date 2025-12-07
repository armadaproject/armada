import { OIDC_REDIRECT } from "../pathnames"

import { Config, LookoutUiConfig } from "./types"

export const DEFAULT_LOOKOUT_UI_CONFIG: LookoutUiConfig = {
  armadaApiBaseUrl: "",
  userAnnotationPrefix: "",
  binocularsBaseUrlPattern: "",
  jobSetsAutoRefreshMs: undefined,
  jobsAutoRefreshMs: undefined,
  customTitle: "",
  oidcEnabled: false,
  oidc: undefined,
  commandSpecs: [],
  jobLinks: [],
  backend: undefined,
  pinnedTimeZoneIdentifiers: [],
  errorMonitoring: {
    sentry: undefined,
  },
  customThemeConfigs: undefined,
}

export const getConfig = (): Config => {
  if (!window.__LOOKOUT_UI_CONFIG__) {
    throw new Error("Unable to load Lookout UI configuration from /lookout-ui-config.js")
  }

  const lookoutUiConfig = { ...DEFAULT_LOOKOUT_UI_CONFIG, ...window.__LOOKOUT_UI_CONFIG__ }

  const searchParams = new URLSearchParams(window.location.search)

  // Allow user to override 'backend' config parameter via a search param
  if (searchParams.has("backend")) {
    lookoutUiConfig.backend = searchParams.get("backend")!
  }

  // Allow user to override 'oidcEnabled' config parameter via a search param
  if (searchParams.get("oidcEnabled") === JSON.stringify(false)) {
    lookoutUiConfig.oidcEnabled = false
  }
  if (searchParams.get("oidcEnabled") === JSON.stringify(true)) {
    lookoutUiConfig.oidcEnabled = true
  }

  // Override 'oidcEnabled' if the pathname is the OIDC redirect path, ensuring it is set to true
  if (window.location.pathname === OIDC_REDIRECT) {
    lookoutUiConfig.oidcEnabled = true
  }

  return {
    ...lookoutUiConfig,
    debugEnabled: searchParams.has("debug"),
    fakeDataEnabled: searchParams.has("fakeData"),
  }
}
