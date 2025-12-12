import { useMemo } from "react"

import { QueryFunction, QueryKey, useQuery } from "@tanstack/react-query"

import { getErrorMessage } from "../../common/utils"
import { getConfig } from "../../config"
import { Job, JobFilter, JobOrder } from "../../models/lookoutModels"
import { useAuthenticatedFetch } from "../../oidcAuth"

export interface GetJobsParams {
  filters: JobFilter[]
  activeJobSets: boolean
  order: JobOrder
  skip: number
  take: number
}

export interface GetJobsResponse {
  jobs: Job[]
}

const getQueryFn =
  (
    params: GetJobsParams,
    fetchFunc: GlobalFetch["fetch"],
    backend: string | undefined,
    fakeDataEnabled: boolean,
  ): QueryFunction<GetJobsResponse, QueryKey, never> =>
  async ({ signal }) => {
    try {
      if (fakeDataEnabled) {
        return { jobs: [] }
      }

      let path = "/api/v1/jobs"
      if (backend) {
        path += "?" + new URLSearchParams({ backend })
      }

      const response = await fetchFunc(path, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          filters: params.filters,
          activeJobSets: params.activeJobSets,
          order: params.order,
          skip: params.skip,
          take: params.take,
        }),
        signal,
      })

      const json = await response.json()
      return {
        jobs: json.jobs ?? [],
      }
    } catch (e) {
      throw await getErrorMessage(e)
    }
  }

export const useGetJobs = (params: GetJobsParams, enabled = true) => {
  const config = getConfig()
  const authenticatedFetch = useAuthenticatedFetch()

  const queryFn = useMemo(
    () => getQueryFn(params, authenticatedFetch, config.backend, config.fakeDataEnabled),
    [params, authenticatedFetch, config.backend, config.fakeDataEnabled],
  )

  return useQuery<GetJobsResponse, string>({
    queryKey: [
      "getJobs",
      params.filters,
      params.activeJobSets,
      params.order,
      params.skip,
      params.take,
      config.backend,
      config.fakeDataEnabled,
    ],
    queryFn,
    enabled,
    refetchOnMount: false,
    staleTime: 30_000,
  })
}
