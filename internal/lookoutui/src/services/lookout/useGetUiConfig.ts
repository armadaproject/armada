import { useQuery } from "@tanstack/react-query"

import { getErrorMessage, OidcConfig, UIConfig } from "../../utils"

const DEFAULT_OIDC_CONFIG: OidcConfig = {
  authority: "",
  clientId: "",
  scope: "",
}

const DEFAULT_UI_CONFIG: UIConfig = {
  armadaApiBaseUrl: "",
  userAnnotationPrefix: "",
  binocularsBaseUrlPattern: "",
  jobSetsAutoRefreshMs: undefined,
  jobsAutoRefreshMs: undefined,
  debugEnabled: false,
  fakeDataEnabled: false,
  customTitle: "",
  oidcEnabled: false,
  oidc: undefined,
  commandSpecs: [],
  backend: undefined,
}

export const useGetUiConfig = (enabled = true) => {
  const searchParams = new URLSearchParams(window.location.search)

  const config = {
    ...DEFAULT_UI_CONFIG,
    debugEnabled: searchParams.has("debug"),
    fakeDataEnabled: searchParams.has("fakeData"),
  }

  return useQuery<UIConfig, string>({
    queryKey: ["useGetUiConfig"],
    queryFn: async ({ signal }) => {
      try {
        const response = await fetch("/config", { signal })
        const json = await response.json()

        if (json.ArmadaApiBaseUrl) config.armadaApiBaseUrl = json.ArmadaApiBaseUrl
        if (json.UserAnnotationPrefix) config.userAnnotationPrefix = json.UserAnnotationPrefix
        if (json.BinocularsBaseUrlPattern) config.binocularsBaseUrlPattern = json.BinocularsBaseUrlPattern
        if (json.JobSetsAutoRefreshMs) config.jobSetsAutoRefreshMs = json.JobSetsAutoRefreshMs
        if (json.JobsAutoRefreshMs) config.jobsAutoRefreshMs = json.JobsAutoRefreshMs
        if (json.CustomTitle) config.customTitle = json.CustomTitle
        if (json.OidcEnabled) config.oidcEnabled = json.OidcEnabled

        if (json.Oidc) {
          config.oidc = DEFAULT_OIDC_CONFIG
          if (json.Oidc.Authority) config.oidc.authority = json.Oidc.Authority
          if (json.Oidc.ClientId) config.oidc.clientId = json.Oidc.ClientId
          if (json.Oidc.Scope) config.oidc.scope = json.Oidc.Scope
        }

        if (json.CommandSpecs) {
          config.commandSpecs = json.CommandSpecs.map(
            ({
              Name,
              Template,
              DescriptionMd,
              AlertMessageMd,
              AlertLevel,
            }: {
              Name: string
              Template: string
              DescriptionMd: string
              AlertMessageMd: string
              AlertLevel: string
            }) => ({
              name: Name,
              template: Template,
              descriptionMd: DescriptionMd,
              alertMessageMd: AlertMessageMd,
              alertLevel: AlertLevel,
            }),
          )
        }

        if (json.Backend) config.backend = json.Backend
      } catch (e) {
        throw await getErrorMessage(e)
      }

      config.oidcEnabled =
        searchParams.get("oidcEnabled") === JSON.stringify(true) || window.location.pathname === "/oidc"

      const backend = searchParams.get("backend")
      if (backend) config.backend = backend

      return config
    },
    enabled,
    refetchOnMount: false,
    staleTime: Infinity,
  })
}
