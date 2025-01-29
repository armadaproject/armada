import { useQuery } from "@tanstack/react-query"

import { useGetUiConfig } from "./useGetUiConfig"
import { getAccessToken, getAuthorizationHeaders, useUserManager } from "../../oidc"
import { SchedulerReportingApi, Configuration, SchedulerobjectsSchedulingReport } from "../../openapi/schedulerobjects"
import { getErrorMessage } from "../../utils"

export const useGetSchedulingReport = (verbosity: number, enabled = true) => {
  const userManager = useUserManager()

  const { data: uiConfig } = useGetUiConfig(enabled)
  const armadaApiBaseUrl = uiConfig?.armadaApiBaseUrl

  const schedulerReportingApiConfiguration: Configuration = new Configuration({
    basePath: armadaApiBaseUrl,
    credentials: "include",
  })
  const schedulerReportingApi = new SchedulerReportingApi(schedulerReportingApiConfiguration)

  return useQuery<SchedulerobjectsSchedulingReport, string>({
    queryKey: ["getSchedulingReport", verbosity],
    queryFn: async ({ signal }) => {
      try {
        const headers: HeadersInit = {}

        if (userManager !== undefined) {
          Object.assign(headers, getAuthorizationHeaders(await getAccessToken(userManager)))
        }

        return await schedulerReportingApi.getSchedulingReport({ verbosity }, { signal })
      } catch (e) {
        throw await getErrorMessage(e)
      }
    },
    enabled: Boolean(enabled && armadaApiBaseUrl),
    refetchOnMount: false,
    staleTime: 30_000,
  })
}
