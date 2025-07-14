import { useMemo } from "react"

import { QueryFunction, QueryKey, useQueries, useQuery } from "@tanstack/react-query"

import { getErrorMessage } from "../../utils"
import { fakeRunError } from "./mocks/fakeData"
import { useGetUiConfig } from "./useGetUiConfig"
import { useAuthenticatedFetch } from "../../oidcAuth"

const getQueryFn =
  (runId: string, fetchFunc: GlobalFetch["fetch"], fakeDataEnabled: boolean): QueryFunction<string, QueryKey, never> =>
  async ({ signal }) => {
    try {
      if (fakeDataEnabled) {
        if (runId === "doesnotexist") {
          throw new Error("Failed to retrieve job run because of reasons")
        }
        return fakeRunError
      }

      const response = await fetchFunc("/api/v1/jobRunError", {
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

export const useGetJobRunError = (runId: string, enabled = true) => {
  const { data: uiConfig } = useGetUiConfig()
  const authenticatedFetch = useAuthenticatedFetch()

  const queryFn = useMemo(
    () => getQueryFn(runId, authenticatedFetch, Boolean(uiConfig?.fakeDataEnabled)),
    [runId, authenticatedFetch, uiConfig?.fakeDataEnabled],
  )

  return useQuery<string, string>({
    queryKey: ["getJobRunError", runId],
    queryFn,
    enabled,
    refetchOnMount: false,
    staleTime: 30_000,
  })
}

export const useBatchGetJobRunErrors = (runIds: string[], enabled = true) => {
  const { data: uiConfig } = useGetUiConfig()
  const authenticatedFetch = useAuthenticatedFetch()

  return useQueries({
    queries: runIds.map((runId) => ({
      queryKey: ["getJobRunError", runId],
      queryFn: getQueryFn(runId, authenticatedFetch, Boolean(uiConfig?.fakeDataEnabled)),
      enabled,
      refetchOnMount: false,
      staleTime: 30_000,
    })),
  })
}
