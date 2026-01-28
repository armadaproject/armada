import { useQuery } from "@tanstack/react-query"

import { getErrorMessage } from "../../common/utils"
import { getConfig } from "../../config"
import { SchedulerobjectsJobReport } from "../../openapi/schedulerobjects"

import { useApiClients } from "../apiClients"

import { fakeSchedulingReport } from "./mocks/fakeData"

export const useGetJobSchedulingReport = (jobId: string, enabled = true) => {
  const config = getConfig()
  const { schedulerReportingApi } = useApiClients()

  return useQuery<SchedulerobjectsJobReport, string>({
    queryKey: ["getJobSchedulingReport", jobId, config.fakeDataEnabled],
    queryFn: async ({ signal }) => {
      if (config.fakeDataEnabled) {
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
