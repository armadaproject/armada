import { useCallback } from "react"

import { getErrorMessage } from "../../common/utils"
import { getConfig } from "../../config"
import { AggregateType, JobFilter, JobGroup, JobOrder } from "../../models/lookoutModels"
import { useAuthenticatedFetch } from "../../oidcAuth"

export type GroupedField = {
  field: string
  isAnnotation: boolean
  lastTransitionTimeAggregate?: AggregateType
}

export type GroupJobsResponse = {
  groups: JobGroup[]
}

export const useGroupJobs = () => {
  const authenticatedFetch = useAuthenticatedFetch()
  const config = getConfig()

  return useCallback(
    async (
      filters: JobFilter[],
      activeJobSets: boolean,
      order: JobOrder,
      groupedField: GroupedField,
      aggregates: string[],
      skip: number,
      take: number,
      abortSignal?: AbortSignal,
    ): Promise<GroupJobsResponse> => {
      if (config.fakeDataEnabled) {
        return { groups: [] }
      }

      try {
        let path = "/api/v1/jobGroups"
        if (config.backend) {
          path += "?" + new URLSearchParams({ backend: config.backend })
        }

        const response = await authenticatedFetch(path, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            filters,
            activeJobSets,
            order,
            groupedField,
            aggregates,
            skip,
            take,
          }),
          signal: abortSignal,
        })

        const json = await response.json()
        return {
          groups: json.groups ?? [],
        }
      } catch (e) {
        throw await getErrorMessage(e)
      }
    },
    [authenticatedFetch, config.backend, config.fakeDataEnabled],
  )
}
