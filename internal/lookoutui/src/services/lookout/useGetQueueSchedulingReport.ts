import { useQuery } from "@tanstack/react-query"

import { SchedulerobjectsQueueReport } from "../../openapi/schedulerobjects"
import { getErrorMessage } from "../../utils"
import { useApiClients } from "../apiClients"
import { fakeSchedulingReport } from "./mocks/fakeData"
import { getConfig } from "../../config"

export const useGetQueueSchedulingReport = (queueName: string, verbosity: number, enabled = true) => {
  const config = getConfig()
  const { schedulerReportingApi } = useApiClients()

  return useQuery<SchedulerobjectsQueueReport, string>({
    queryKey: ["getQueueSchedulingReport", queueName, verbosity, config.fakeDataEnabled],
    queryFn: async ({ signal }) => {
      if (config.fakeDataEnabled) {
        return { report: fakeSchedulingReport }
      }

      try {
        return await schedulerReportingApi.getQueueReport({ queueName, verbosity }, { signal })
      } catch (e) {
        throw await getErrorMessage(e)
      }
    },
    enabled,
    refetchOnMount: false,
    staleTime: 30_000,
  })
}
