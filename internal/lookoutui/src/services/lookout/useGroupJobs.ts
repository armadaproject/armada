import { useCallback, useMemo } from "react"

import { makeRandomJobs, mergeFilters } from "../../common/fakeJobsUtils"
import { getErrorMessage } from "../../common/utils"
import { getConfig } from "../../config"
import { AggregateType, Job, JobFilter, JobGroup, JobOrder, JobState } from "../../models/lookoutModels"
import { useAuthenticatedFetch } from "../../oidcAuth"

export type GroupedField = {
  field: string
  isAnnotation: boolean
  lastTransitionTimeAggregate?: AggregateType
}

export type GroupJobsResponse = {
  groups: JobGroup[]
}

let fakeJobsCache: Job[] | undefined

function getFakeJobs(): Job[] {
  if (fakeJobsCache === undefined) {
    fakeJobsCache = makeRandomJobs(10_000, 42)
  }
  return fakeJobsCache
}

function groupFakeJobs(
  jobs: Job[],
  filters: JobFilter[],
  activeJobSets: boolean,
  groupedField: GroupedField,
  skip: number,
  take: number,
): JobGroup[] {
  let filtered = jobs.filter(mergeFilters(filters))
  if (activeJobSets) {
    const activeStates = new Set([JobState.Queued, JobState.Leased, JobState.Pending, JobState.Running])
    filtered = filtered.filter((j) => activeStates.has(j.state))
  }

  const grouped = new Map<string, Job[]>()
  for (const job of filtered) {
    const key = groupedField.isAnnotation
      ? (job.annotations[groupedField.field] ?? "")
      : String((job as Record<string, unknown>)[groupedField.field] ?? "")
    if (!grouped.has(key)) {
      grouped.set(key, [])
    }
    grouped.get(key)!.push(job)
  }

  return [...grouped.entries()].slice(skip, skip + take).map(([name, groupJobs]) => {
    const stateCounts: Record<string, number> = {}
    for (const j of groupJobs) {
      stateCounts[j.state] = (stateCounts[j.state] ?? 0) + 1
    }
    return {
      name,
      count: groupJobs.length,
      aggregates: { state: stateCounts },
    }
  })
}

export const useGroupJobs = () => {
  const authenticatedFetch = useAuthenticatedFetch()
  const config = getConfig()
  const fakeJobs = useMemo(() => (config.fakeDataEnabled ? getFakeJobs() : []), [config.fakeDataEnabled])

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
        return { groups: groupFakeJobs(fakeJobs, filters, activeJobSets, groupedField, skip, take) }
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
    [authenticatedFetch, config.backend, config.fakeDataEnabled, fakeJobs],
  )
}
