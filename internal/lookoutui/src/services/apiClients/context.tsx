import { createContext, ReactNode, useCallback, useContext, useMemo, useRef } from "react"

import { getConfig } from "../../config"
import { useAuthenticatedFetch } from "../../oidcAuth"
import { Configuration as ArmadaConfiguration, SubmitApi } from "../../openapi/armada"
import { BinocularsApi, Configuration as BinocularsConfiguration } from "../../openapi/binoculars"
import { SchedulerReportingApi, Configuration as SchedulerObjectsConfiguration } from "../../openapi/schedulerobjects"

export interface ApiClients {
  submitApi: SubmitApi
  schedulerReportingApi: SchedulerReportingApi
  getBinocularsApi: (clusterId: string) => BinocularsApi
}

interface ApiClientsContextValue {
  apiClients: ApiClients
}

const ApiClientsContext = createContext<ApiClientsContextValue | undefined>(undefined)

export interface ApiClientsProviderProps {
  children: ReactNode
}

export const ApiClientsProvider = ({ children }: ApiClientsProviderProps) => {
  const config = getConfig()

  const authenticatedFetch = useAuthenticatedFetch()

  const schedulerReportingApi = useMemo(() => {
    const schedulerReportingApiConfiguration = new SchedulerObjectsConfiguration({
      basePath: config.armadaApiBaseUrl,
      credentials: "include",
      fetchApi: authenticatedFetch,
    })
    return new SchedulerReportingApi(schedulerReportingApiConfiguration)
  }, [config.armadaApiBaseUrl])

  const submitApi = useMemo(() => {
    const submitApiConfiguration = new ArmadaConfiguration({
      basePath: config.armadaApiBaseUrl,
      credentials: "include",
      fetchApi: authenticatedFetch,
    })
    return new SubmitApi(submitApiConfiguration)
  }, [config.armadaApiBaseUrl])

  const binocularsApiCacheRef = useRef<{
    binocularsBaseUrlPattern: string
    binocularsStaticBaseUrls: Record<string, string>
    binocularsApis: Record<string, BinocularsApi>
  }>({
    binocularsBaseUrlPattern: config.binocularsBaseUrlPattern ?? "",
    binocularsStaticBaseUrls: config.binocularsStaticBaseUrls,
    binocularsApis: {},
  })
  const getBinocularsApi = useCallback(
    (clusterId: string) => {
      const cache = binocularsApiCacheRef.current
      if (
        cache.binocularsBaseUrlPattern !== (config.binocularsBaseUrlPattern ?? "") ||
        cache.binocularsStaticBaseUrls !== config.binocularsStaticBaseUrls
      ) {
        cache.binocularsBaseUrlPattern = config.binocularsBaseUrlPattern ?? ""
        cache.binocularsStaticBaseUrls = config.binocularsStaticBaseUrls
        cache.binocularsApis = {}
      }

      if (!cache.binocularsApis[clusterId]) {
        cache.binocularsApis[clusterId] = new BinocularsApi(
          new BinocularsConfiguration({
            basePath: resolveBinocularsBaseUrl(
              clusterId,
              config.binocularsStaticBaseUrls,
              config.binocularsBaseUrlPattern,
            ),
            credentials: "include",
            fetchApi: authenticatedFetch,
          }),
        )
      }
      return cache.binocularsApis[clusterId]
    },
    [binocularsApiCacheRef, config.binocularsBaseUrlPattern, config.binocularsStaticBaseUrls],
  )

  const apiClients = useMemo(
    () => ({ getBinocularsApi, schedulerReportingApi, submitApi }),
    [getBinocularsApi, schedulerReportingApi, submitApi],
  )

  return <ApiClientsContext.Provider value={{ apiClients }}>{children}</ApiClientsContext.Provider>
}

export const useApiClients = () => {
  const context = useContext(ApiClientsContext)
  if (context === undefined) {
    throw new Error("useApiClients() must be used within a ApiClientsProvider")
  }
  return context.apiClients
}

// resolveBinocularsBaseUrl returns the Binoculars base URL for a cluster: its entry in staticBaseUrls if it has one,
// otherwise the pattern with {CLUSTER_ID} replaced. Cluster IDs are matched case-insensitively, because the Lookout
// server's config loader lower-cases the keys of maps such as binocularsStaticBaseUrls.
const resolveBinocularsBaseUrl = (
  clusterId: string,
  staticBaseUrls: Record<string, string>,
  pattern: string,
): string => {
  const wanted = clusterId.toLowerCase()
  for (const [id, url] of Object.entries(staticBaseUrls)) {
    if (id.toLowerCase() === wanted) {
      return url
    }
  }
  return pattern.replace("{CLUSTER_ID}", clusterId)
}
