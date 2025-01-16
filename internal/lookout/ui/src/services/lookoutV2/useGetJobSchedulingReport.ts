import { useQuery } from "@tanstack/react-query"

import { useGetUiConfig } from "./useGetUiConfig"
import { SchedulerReportingApi, Configuration, SchedulerobjectsJobReport } from "../../openapi/schedulerobjects"
import { getErrorMessage } from "../../utils"

export const useGetJobSchedulingReport = (jobId: string, enabled = true) => {
  const { data: uiConfig } = useGetUiConfig(enabled)
  const armadaApiBaseUrl = uiConfig?.armadaApiBaseUrl

  const schedulerReportingApiConfiguration: Configuration = new Configuration({ basePath: armadaApiBaseUrl })
  const schedulerReportingApi = new SchedulerReportingApi(schedulerReportingApiConfiguration)

  return useQuery<SchedulerobjectsJobReport, string>({
    queryKey: ["getJobSchedulingReport", jobId],
    queryFn: async ({ signal }) => {
      try {
        return await schedulerReportingApi.getJobReport({ jobId }, { signal })
      } catch (e) {
        throw await getErrorMessage(e)
      }
    },
    enabled: Boolean(enabled && armadaApiBaseUrl),
    refetchOnMount: false,
    staleTime: 30_000,
  })
}
