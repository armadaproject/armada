import { useMemo } from "react"

import { QueryFunction, QueryKey, useQueries, useQuery } from "@tanstack/react-query"

import { getErrorMessage } from "../../common/utils"
import { getConfig } from "../../config"
import { useMirroredLookoutApiFetch } from "../../oidcAuth"

import { fakeRunError } from "./mocks/fakeData"

const getQueryFn =
  (runId: string, fetchFunc: GlobalFetch["fetch"], fakeDataEnabled: boolean): QueryFunction<string, QueryKey, never> =>
  async ({ signal }) => {
    try {
      if (fakeDataEnabled) {
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
  const config = getConfig()
  const lookoutApiFetch = useMirroredLookoutApiFetch()

  const queryFn = useMemo(
    () => getQueryFn(runId, lookoutApiFetch, config.fakeDataEnabled),
    [runId, lookoutApiFetch, config.fakeDataEnabled],
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
  const config = getConfig()
  const lookoutApiFetch = useMirroredLookoutApiFetch()

  return useQueries({
    queries: runIds.map((runId) => ({
      queryKey: ["getJobRunError", runId],
      queryFn: getQueryFn(runId, lookoutApiFetch, config.fakeDataEnabled),
      enabled,
      refetchOnMount: false,
      staleTime: 30_000,
    })),
  })
}
