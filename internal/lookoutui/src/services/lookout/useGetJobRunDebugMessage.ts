import { useMemo } from "react"

import { QueryFunction, QueryKey, useQueries, useQuery } from "@tanstack/react-query"

import { getErrorMessage } from "../../common/utils"
import { getConfig } from "../../config"
import { useMirroredLookoutApiFetch } from "../../oidcAuth"
import { JobRunDebugMessage, JobRunDebugMessageFromJSON } from "../../openapi/lookout"

import { fakeRunDebugMessage } from "./mocks/fakeData"

const getQueryFn =
  (
    runId: string,
    fetchFunc: GlobalFetch["fetch"],
    fakeDataEnabled: boolean,
  ): QueryFunction<JobRunDebugMessage, QueryKey, never> =>
  async ({ signal }) => {
    try {
      if (fakeDataEnabled) {
        return { errorString: fakeRunDebugMessage }
      }

      const response = await fetchFunc("/api/v1/jobRunDebugMessage", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ runId }),
        signal,
      })

      return JobRunDebugMessageFromJSON(await response.json())
    } catch (e) {
      throw await getErrorMessage(e)
    }
  }

export const useGetJobRunDebugMessage = (runId: string, enabled = true) => {
  const config = getConfig()

  const lookoutApiFetch = useMirroredLookoutApiFetch()

  const queryFn = useMemo(
    () => getQueryFn(runId, lookoutApiFetch, config.fakeDataEnabled),
    [runId, lookoutApiFetch, config.fakeDataEnabled],
  )

  return useQuery<JobRunDebugMessage, string>({
    queryKey: ["getJobRunDebugMessage", runId],
    queryFn,
    enabled,
    refetchOnMount: false,
    staleTime: 30_000,
  })
}

export const useBatchGetJobRunDebugMessages = (runIds: string[], enabled = true) => {
  const config = getConfig()
  const lookoutApiFetch = useMirroredLookoutApiFetch()

  return useQueries({
    queries: runIds.map((runId) => ({
      queryKey: ["getJobRunDebugMessage", runId],
      queryFn: getQueryFn(runId, lookoutApiFetch, config.fakeDataEnabled),
      enabled,
      refetchOnMount: false,
      staleTime: 30_000,
    })),
  })
}
