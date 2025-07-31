import { useMemo } from "react"

import { QueryFunction, QueryKey, useQueries, useQuery } from "@tanstack/react-query"

import { useAuthenticatedFetch } from "../../oidcAuth"
import { getErrorMessage } from "../../utils"
import { fakeRunDebugMessage } from "./mocks/fakeData"
import { getConfig } from "../../config"

const getQueryFn =
  (runId: string, fetchFunc: GlobalFetch["fetch"], fakeDataEnabled: boolean): QueryFunction<string, QueryKey, never> =>
  async ({ signal }) => {
    try {
      if (fakeDataEnabled) {
        if (runId === "doesnotexist") {
          throw new Error("Failed to retrieve job run because of reasons")
        }
        return fakeRunDebugMessage
      }

      const response = await fetchFunc("/api/v1/jobRunDebugMessage", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ runId }),
        signal,
      })

      const json = await response.json()
      return json.errorString ?? ""
    } catch (e) {
      throw await getErrorMessage(e)
    }
  }

export const useGetJobRunDebugMessage = (runId: string, enabled = true) => {
  const config = getConfig()

  const authenticatedFetch = useAuthenticatedFetch()

  const queryFn = useMemo(
    () => getQueryFn(runId, authenticatedFetch, config.fakeDataEnabled),
    [runId, authenticatedFetch, config.fakeDataEnabled],
  )

  return useQuery<string, string>({
    queryKey: ["getJobRunDebugMessage", runId],
    queryFn,
    enabled,
    refetchOnMount: false,
    staleTime: 30_000,
  })
}

export const useBatchGetJobRunDebugMessages = (runIds: string[], enabled = true) => {
  const config = getConfig()
  const authenticatedFetch = useAuthenticatedFetch()

  return useQueries({
    queries: runIds.map((runId) => ({
      queryKey: ["getJobRunDebugMessage", runId],
      queryFn: getQueryFn(runId, authenticatedFetch, config.fakeDataEnabled),
      enabled,
      refetchOnMount: false,
      staleTime: 30_000,
    })),
  })
}
