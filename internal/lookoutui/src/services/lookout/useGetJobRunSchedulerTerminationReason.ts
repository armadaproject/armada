import { useMemo } from "react"

import { QueryFunction, QueryKey, useQuery } from "@tanstack/react-query"

import { getErrorMessage } from "../../common/utils"
import { getConfig } from "../../config"
import { useMirroredLookoutApiFetch } from "../../oidcAuth"

const getQueryFn =
  (runId: string, fetchFunc: GlobalFetch["fetch"], fakeDataEnabled: boolean): QueryFunction<string, QueryKey, never> =>
  async ({ signal }) => {
    try {
      if (fakeDataEnabled) {
        return ""
      }

      const response = await fetchFunc("/api/v1/jobRunSchedulerTerminationReason", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ runId }),
        signal,
      })

      const json = await response.json()
      return json.schedulerTerminationReason ?? ""
    } catch (e) {
      throw await getErrorMessage(e)
    }
  }

export const useGetJobRunSchedulerTerminationReason = (runId: string, enabled = true) => {
  const config = getConfig()
  const lookoutApiFetch = useMirroredLookoutApiFetch()

  const queryFn = useMemo(
    () => getQueryFn(runId, lookoutApiFetch, config.fakeDataEnabled),
    [runId, lookoutApiFetch, config.fakeDataEnabled],
  )

  return useQuery<string, string>({
    queryKey: ["getJobRunSchedulerTerminationReason", runId],
    queryFn,
    enabled,
    refetchOnMount: false,
    staleTime: 30_000,
  })
}
