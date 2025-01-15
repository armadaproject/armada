import { useQuery } from "@tanstack/react-query"

import { useGetUiConfig } from "./useGetUiConfig"
import { SchedulerReportingApi, Configuration, SchedulerobjectsSchedulingReport } from "../../openapi/schedulerobjects"
import { getErrorMessage } from "../../utils"

export const useGetSchedulingReport = (verbosity: number, enabled = true) => {
  const { data: uiConfig } = useGetUiConfig(enabled)
  const armadaApiBaseUrl = uiConfig?.armadaApiBaseUrl

  const schedulerReportingApiConfiguration: Configuration = new Configuration({ basePath: armadaApiBaseUrl })
  const schedulerReportingApi = new SchedulerReportingApi(schedulerReportingApiConfiguration)

  return useQuery<SchedulerobjectsSchedulingReport, string>({
    queryKey: ["getSchedulingReport", verbosity],
    queryFn: async ({ signal }) => {
      try {
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
