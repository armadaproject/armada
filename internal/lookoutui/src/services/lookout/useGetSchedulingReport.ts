import { useQuery } from "@tanstack/react-query"

import { SchedulerobjectsSchedulingReport } from "../../openapi/schedulerobjects"
import { getErrorMessage } from "../../utils"
import { useApiClients } from "../apiClients"
import { fakeSchedulingReport } from "./mocks/fakeData"
import { getConfig } from "../../config"

export const useGetSchedulingReport = (verbosity: number, enabled = true) => {
  const config = getConfig()
  const { schedulerReportingApi } = useApiClients()

  return useQuery<SchedulerobjectsSchedulingReport, string>({
    queryKey: ["getSchedulingReport", verbosity, config.fakeDataEnabled],
    queryFn: async ({ signal }) => {
      if (config.fakeDataEnabled) {
        return { report: fakeSchedulingReport }
      }

      try {
        return await schedulerReportingApi.getSchedulingReport({ verbosity }, { signal })
      } catch (e) {
        throw await getErrorMessage(e)
      }
    },
    enabled,
    refetchOnMount: false,
    staleTime: 30_000,
  })
}
