import { useQuery } from "@tanstack/react-query"

import { SchedulerobjectsJobReport } from "../../openapi/schedulerobjects"
import { getErrorMessage } from "../../utils"
import { useApiClients } from "../apiClients"
import { fakeSchedulingReport } from "./mocks/fakeData"
import { useGetUiConfig } from "./useGetUiConfig"

export const useGetJobSchedulingReport = (jobId: string, enabled = true) => {
  const { data: uiConfig } = useGetUiConfig()
  const { schedulerReportingApi } = useApiClients()

  return useQuery<SchedulerobjectsJobReport, string>({
    queryKey: ["getJobSchedulingReport", jobId],
    queryFn: async ({ signal }) => {
      if (uiConfig?.fakeDataEnabled) {
        return { report: fakeSchedulingReport }
      }

      try {
        return await schedulerReportingApi.getJobReport({ jobId }, { signal })
      } catch (e) {
        throw await getErrorMessage(e)
      }
    },
    enabled,
    refetchOnMount: false,
    staleTime: 30_000,
  })
}
