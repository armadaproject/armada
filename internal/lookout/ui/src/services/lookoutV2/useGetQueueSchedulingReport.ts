import { useQuery } from "@tanstack/react-query"

import { useGetUiConfig } from "./useGetUiConfig"
import { SchedulerReportingApi, Configuration, SchedulerobjectsQueueReport } from "../../openapi/schedulerobjects"
import { getErrorMessage } from "../../utils"

export const useGetQueueSchedulingReport = (queueName: string, verbosity: number, enabled = true) => {
  const { data: uiConfig } = useGetUiConfig(enabled)
  const armadaApiBaseUrl = uiConfig?.armadaApiBaseUrl

  const schedulerReportingApiConfiguration: Configuration = new Configuration({ basePath: armadaApiBaseUrl })
  const schedulerReportingApi = new SchedulerReportingApi(schedulerReportingApiConfiguration)

  return useQuery<SchedulerobjectsQueueReport, string>({
    queryKey: ["getQueueSchedulingReport", queueName, verbosity],
    queryFn: async ({ signal }) => {
      try {
        return await schedulerReportingApi.getQueueReport({ queueName, verbosity }, { signal })
      } catch (e) {
        throw await getErrorMessage(e)
      }
    },
    enabled: Boolean(enabled && armadaApiBaseUrl),
    refetchOnMount: false,
    staleTime: 30_000,
  })
}
