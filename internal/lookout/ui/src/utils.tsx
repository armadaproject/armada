import { Location, NavigateFunction, Params, useLocation, useNavigate, useParams } from "react-router-dom"

import { BinocularsApi, Configuration, ConfigurationParameters } from "./openapi/binoculars"

export interface OidcConfig {
  authority: string
  clientId: string
  scope: string
}

interface UIConfig {
  armadaApiBaseUrl: string
  userAnnotationPrefix: string
  binocularsBaseUrlPattern: string
  jobSetsAutoRefreshMs: number
  jobsAutoRefreshMs: number
  debugEnabled: boolean
  fakeDataEnabled: boolean
  customTitle: string
  oidcEnabled: boolean
  oidc?: OidcConfig
}

export type RequestStatus = "Loading" | "Idle"

export type ApiResult = "Success" | "Failure" | "Partial success"

export interface Padding {
  top: number
  bottom: number
  left: number
  right: number
}

export async function getUIConfig(): Promise<UIConfig> {
  const searchParams = new URLSearchParams(window.location.search)

  const config: UIConfig = {
    armadaApiBaseUrl: "",
    userAnnotationPrefix: "",
    binocularsBaseUrlPattern: "",
    jobSetsAutoRefreshMs: 15000,
    jobsAutoRefreshMs: 30000,
    debugEnabled: searchParams.has("debug"),
    fakeDataEnabled: searchParams.has("fakeData"),
    customTitle: "",
    oidcEnabled: false,
    oidc: undefined,
  }

  try {
    const response = await fetch("/config")
    const json = await response.json()
    if (json.ArmadaApiBaseUrl) config.armadaApiBaseUrl = json.ArmadaApiBaseUrl
    if (json.UserAnnotationPrefix) config.userAnnotationPrefix = json.UserAnnotationPrefix
    if (json.BinocularsBaseUrlPattern) config.binocularsBaseUrlPattern = json.BinocularsBaseUrlPattern
    if (json.JobSetsAutoRefreshMs) config.jobSetsAutoRefreshMs = json.JobSetsAutoRefreshMs
    if (json.JobsAutoRefreshMs) config.jobsAutoRefreshMs = json.JobsAutoRefreshMs
    if (json.CustomTitle) config.customTitle = json.CustomTitle
    if (json.OidcEnabled) config.oidcEnabled = json.OidcEnabled
    if (json.Oidc) {
      config.oidc = {
        authority: json.Oidc.Authority,
        clientId: json.Oidc.ClientId,
        scope: json.Oidc.Scope,
      }
    }
  } catch (e) {
    console.error(e)
  }

  switch (searchParams.get("oidcEnabled")) {
    case "false":
      config.oidcEnabled = false
      break
    case "true":
      config.oidcEnabled = true
      break
  }

  if (window.location.pathname === "/oidc") config.oidcEnabled = true

  return config
}

export function inverseRecord<K extends string | number | symbol, V extends string | number | symbol>(
  record: Record<K, V>,
): Record<V, K> {
  return Object.fromEntries(Object.entries(record).map(([k, v]) => [v, k]))
}

export function debounced(fn: (...args: any[]) => Promise<any>, delay: number): (...args: any[]) => Promise<any> {
  let timerId: NodeJS.Timeout | null
  return function (...args: any[]): Promise<any> {
    return new Promise<any>((resolve) => {
      if (timerId) {
        clearTimeout(timerId)
      }
      timerId = setTimeout(() => {
        resolve(fn(...args))
        timerId = null
      }, delay)
    })
  }
}

export function setStateAsync<T>(component: React.Component<any, T>, state: T): Promise<void> {
  return new Promise((resolve) => {
    component.setState(state, resolve)
  })
}

export function selectItem<V>(key: string, item: V, selectedMap: Map<string, V>, isSelected: boolean) {
  if (isSelected) {
    selectedMap.set(key, item)
  } else if (selectedMap.has(key)) {
    selectedMap.delete(key)
  }
}

export async function getErrorMessage(error: any): Promise<string> {
  if (error === undefined) {
    return "Unknown error"
  }
  let basicMessage = (error.status ?? "") + " " + (error.statusText ?? "")
  if (basicMessage === " ") {
    if (error.toString() !== undefined && typeof error.toString === "function") {
      basicMessage = error.toString()
    } else {
      basicMessage = "Unknown error"
    }
  }
  try {
    const json = await error.json()
    const errorMessage = json.message
    return errorMessage ?? basicMessage
  } catch {
    return basicMessage
  }
}

export function tryParseJson(json: string): Record<string, unknown> | unknown[] | undefined {
  try {
    return JSON.parse(json) as Record<string, unknown>
  } catch (e: unknown) {
    if (e instanceof Error) {
      console.error(e.message)
    }
    return undefined
  }
}

const priorityRegex = new RegExp("^([0-9]+)$")

export function priorityIsValid(priority: string): boolean {
  return priorityRegex.test(priority) && priority.length > 0
}

// Pluralization helper
export function pl(itemsOrCount: unknown[] | number, singularForm: string, pluralForm?: string) {
  const count = Array.isArray(itemsOrCount) ? itemsOrCount.length : itemsOrCount
  if (count === 1) {
    return `${count} ${singularForm}`
  }

  if (pluralForm !== undefined) {
    return `${count} ${pluralForm}`
  }

  if (/[s|ss|sh|ch|x|z]$/.test(singularForm)) {
    pluralForm = singularForm + "es"
  } else {
    pluralForm = singularForm + "s"
  }

  return `${count} ${pluralForm}`
}

export async function waitMillis(millisToWait: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, millisToWait))
}

export function removeUndefined(obj: Record<string, any>) {
  return Object.keys(obj).forEach((key) => {
    if (obj[key] === undefined) {
      delete obj[key]
    }
  })
}

export interface Router {
  location: Location
  navigate: NavigateFunction
  params: Readonly<Params>
}

export interface PropsWithRouter {
  router: Router
}

export function withRouter<T extends PropsWithRouter>(Component: React.FC<T>): React.FC<Omit<T, "router">> {
  function ComponentWithRouterProp(props: T) {
    const location = useLocation()
    const navigate = useNavigate()
    const params = useParams()
    return <Component {...props} router={{ location, navigate, params }} />
  }
  return ComponentWithRouterProp as React.FC<Omit<T, "router">>
}

export const PlatformCancelReason = "Platform error marked by user"

export function getBinocularsApi(clusterId: string, baseUrlPattern: string, config: ConfigurationParameters) {
  return new BinocularsApi(
    new Configuration({
      ...config,
      basePath: baseUrlPattern.replace("{CLUSTER_ID}", clusterId),
    }),
  )
}
