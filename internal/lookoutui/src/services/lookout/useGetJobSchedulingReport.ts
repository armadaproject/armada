import { useQuery } from "@tanstack/react-query"

import { useGetUiConfig } from "./useGetUiConfig"
import { appendAuthorizationHeaders, useGetAccessToken } from "../../oidcAuth"
import { SchedulerReportingApi, Configuration, SchedulerobjectsJobReport } from "../../openapi/schedulerobjects"
import { getErrorMessage } from "../../utils"

export const useGetJobSchedulingReport = (jobId: string, enabled = true) => {
  const getAccessToken = useGetAccessToken()

  const { data: uiConfig } = useGetUiConfig(enabled)
  const armadaApiBaseUrl = uiConfig?.armadaApiBaseUrl

  const schedulerReportingApiConfiguration: Configuration = new Configuration({
    basePath: armadaApiBaseUrl,
    credentials: "include",
  })
  const schedulerReportingApi = new SchedulerReportingApi(schedulerReportingApiConfiguration)

  return useQuery<SchedulerobjectsJobReport, string>({
    queryKey: ["getJobSchedulingReport", jobId],
    queryFn: async ({ signal }) => {
      try {
        const accessToken = await getAccessToken()
        const headers = new Headers()
        if (accessToken) {
          appendAuthorizationHeaders(headers, accessToken)
        }

        return await schedulerReportingApi.getJobReport({ jobId }, { headers, signal })
      } catch (e) {
        throw await getErrorMessage(e)
      }
    },
    enabled: Boolean(enabled && armadaApiBaseUrl),
    refetchOnMount: false,
    staleTime: 30_000,
  })
}
