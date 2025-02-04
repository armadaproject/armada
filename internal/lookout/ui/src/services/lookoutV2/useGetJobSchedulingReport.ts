import { useQuery } from "@tanstack/react-query"

import { useGetUiConfig } from "./useGetUiConfig"
import { getAccessToken, getAuthorizationHeaders, useUserManager } from "../../oidc"
import { SchedulerReportingApi, Configuration, SchedulerobjectsJobReport } from "../../openapi/schedulerobjects"
import { getErrorMessage } from "../../utils"

export const useGetJobSchedulingReport = (jobId: string, enabled = true) => {
  const userManager = useUserManager()

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
        const accessToken = userManager === undefined ? undefined : await getAccessToken(userManager)

        return await schedulerReportingApi.getJobReport(
          { jobId },
          { headers: accessToken ? getAuthorizationHeaders(accessToken) : undefined, signal },
        )
      } catch (e) {
        throw await getErrorMessage(e)
      }
    },
    enabled: Boolean(enabled && armadaApiBaseUrl),
    refetchOnMount: false,
    staleTime: 30_000,
  })
}
