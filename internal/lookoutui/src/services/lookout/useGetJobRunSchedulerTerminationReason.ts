import { useMemo } from "react"

import { QueryFunction, QueryKey, useQueries, useQuery } from "@tanstack/react-query"

import { getErrorMessage } from "../../common/utils"
import { getConfig } from "../../config"
import { useAuthenticatedFetch } from "../../oidcAuth"

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
  const authenticatedFetch = useAuthenticatedFetch()

  const queryFn = useMemo(
    () => getQueryFn(runId, authenticatedFetch, config.fakeDataEnabled),
    [runId, authenticatedFetch, config.fakeDataEnabled],
  )

  return useQuery<string, string>({
    queryKey: ["getJobRunSchedulerTerminationReason", runId],
    queryFn,
    enabled,
    refetchOnMount: false,
    staleTime: 30_000,
  })
}

export const useBatchGetJobRunSchedulerTerminationReasons = (runIds: string[], enabled = true) => {
  const config = getConfig()
  const authenticatedFetch = useAuthenticatedFetch()

  return useQueries({
    queries: runIds.map((runId) => ({
      queryKey: ["getJobRunSchedulerTerminationReason", runId],
      queryFn: getQueryFn(runId, authenticatedFetch, config.fakeDataEnabled),
      enabled,
      refetchOnMount: false,
      staleTime: 30_000,
    })),
  })
}
