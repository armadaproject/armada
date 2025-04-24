import { createContext, ReactNode, useCallback, useContext, useMemo, useRef } from "react"

import { FullPageError } from "../../containers/FullPageError"
import { FullPageLoading } from "../../containers/FullPageLoading"
import { useAuthenticatedFetch } from "../../oidcAuth"
import { Configuration as ArmadaConfiguration, SubmitApi } from "../../openapi/armada"
import { BinocularsApi, Configuration as BinocularsConfiguration } from "../../openapi/binoculars"
import { SchedulerReportingApi, Configuration as SchedulerObjectsConfiguration } from "../../openapi/schedulerobjects"
import { useGetUiConfig } from "../lookout/useGetUiConfig"

const ELLIPSIS = "\u2026"

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
  const { data: uiConfig, status, error, refetch } = useGetUiConfig()

  const authenticatedFetch = useAuthenticatedFetch()

  const schedulerReportingApi = useMemo(() => {
    const schedulerReportingApiConfiguration = new SchedulerObjectsConfiguration({
      basePath: uiConfig?.armadaApiBaseUrl,
      credentials: "include",
      fetchApi: authenticatedFetch,
    })
    return new SchedulerReportingApi(schedulerReportingApiConfiguration)
  }, [uiConfig?.armadaApiBaseUrl])

  const submitApi = useMemo(() => {
    const submitApiConfiguration = new ArmadaConfiguration({
      basePath: uiConfig?.armadaApiBaseUrl,
      credentials: "include",
      fetchApi: authenticatedFetch,
    })
    return new SubmitApi(submitApiConfiguration)
  }, [uiConfig?.armadaApiBaseUrl])

  const binocularsApiCacheRef = useRef<{
    binocularsBaseUrlPattern: string
    binocularsApis: Record<string, BinocularsApi>
  }>({ binocularsBaseUrlPattern: uiConfig?.binocularsBaseUrlPattern ?? "", binocularsApis: {} })
  const getBinocularsApi = useCallback(
    (clusterId: string) => {
      const cache = binocularsApiCacheRef.current
      if (cache.binocularsBaseUrlPattern !== (uiConfig?.binocularsBaseUrlPattern ?? "")) {
        cache.binocularsBaseUrlPattern = uiConfig?.binocularsBaseUrlPattern ?? ""
        cache.binocularsApis = {}
      }

      if (!cache.binocularsApis[clusterId]) {
        cache.binocularsApis[clusterId] = new BinocularsApi(
          new BinocularsConfiguration({
            basePath: uiConfig?.binocularsBaseUrlPattern.replace("{CLUSTER_ID}", clusterId),
            credentials: "include",
            fetchApi: authenticatedFetch,
          }),
        )
      }
      return cache.binocularsApis[clusterId]
    },
    [binocularsApiCacheRef, uiConfig?.binocularsBaseUrlPattern],
  )

  const apiClients = useMemo(
    () => ({ getBinocularsApi, schedulerReportingApi, submitApi }),
    [getBinocularsApi, schedulerReportingApi, submitApi],
  )

  if (status === "error") {
    return (
      <FullPageError
        errorTitle="There was a problem retrieving the configuration for Lookout"
        errorMessage={error}
        retry={refetch}
      />
    )
  }

  if (status === "pending") {
    return <FullPageLoading loadingMessage={`Fetching Lookout configuration${ELLIPSIS}`} />
  }

  return <ApiClientsContext.Provider value={{ apiClients }}>{children}</ApiClientsContext.Provider>
}

export const useApiClients = () => {
  const context = useContext(ApiClientsContext)
  if (context === undefined) {
    throw new Error("useApiClients() must be used within a ApiClientsProvider")
  }
  return context.apiClients
}
