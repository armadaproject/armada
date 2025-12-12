import { useCallback, useEffect, useMemo, useState } from "react"

import _ from "lodash"

import { getConfig } from "../../config"
import { Job, JobFilter, JobFiltersWithExcludes } from "../../models/lookoutModels"
import { useAuthenticatedFetch } from "../../oidcAuth"

const MAX_JOBS_PER_REQUEST = 10_000

interface UseGetAllJobsMatchingFiltersOptions {
  filtersGroups: JobFiltersWithExcludes[]
  activeJobSets: boolean
  enabled?: boolean
}

/**
 * Hook to fetch all jobs matching multiple filter groups with pagination.
 *
 * It handles:
 * - Multiple filter groups with exclusion filters
 * - Automatic pagination to fetch all matching jobs
 * - Deduplication of jobs across filter groups
 * - Sorting by jobId descending
 *
 * Note: This uses direct fetch calls rather than useGetJobs hook because we need
 * to paginate imperatively (we don't know how many pages until we fetch them).
 * However, it still uses the same authenticated fetch and config as the rest of the app.
 */
export const useGetAllJobsMatchingFilters = ({
  filtersGroups,
  activeJobSets,
  enabled = true,
}: UseGetAllJobsMatchingFiltersOptions) => {
  const [allJobs, setAllJobs] = useState<Job[]>([])
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [refetchCounter, setRefetchCounter] = useState(0)

  const authenticatedFetch = useAuthenticatedFetch()
  const config = getConfig()

  // Create a stable key for filtersGroups to avoid unnecessary re-renders
  const filtersKey = useMemo(() => JSON.stringify(filtersGroups), [filtersGroups])

  const fetchSinglePage = useCallback(
    async (filters: JobFilter[], activeJobSets: boolean, skip: number): Promise<{ jobs: Job[] }> => {
      let path = "/api/v1/jobs"
      if (config.backend) {
        path += "?" + new URLSearchParams({ backend: config.backend })
      }

      const response = await authenticatedFetch(path, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          filters,
          activeJobSets,
          order: { direction: "DESC", field: "jobId" },
          skip,
          take: MAX_JOBS_PER_REQUEST,
        }),
      })

      const json = await response.json()
      return {
        jobs: json.jobs ?? [],
      }
    },
    [authenticatedFetch, config.backend],
  )

  const fetchJobsWithPagination = useCallback(
    async (filters: JobFilter[], activeJobSets: boolean): Promise<Job[]> => {
      const receivedJobs: Job[] = []
      let continuePaginating = true

      while (continuePaginating) {
        const response = await fetchSinglePage(filters, activeJobSets, receivedJobs.length)
        const jobs = response.jobs ?? []
        receivedJobs.push(...jobs)

        if (jobs.length < MAX_JOBS_PER_REQUEST) {
          continuePaginating = false
        }
      }

      return receivedJobs
    },
    [fetchSinglePage],
  )

  useEffect(() => {
    if (!enabled) {
      return
    }

    let isCancelled = false

    const fetchAllJobs = async () => {
      setIsLoading(true)
      setError(null)

      try {
        // Fetch jobs for each filter group in parallel
        const jobsBySelectedItem = await Promise.all(
          filtersGroups.map(async ({ jobFilters, excludesJobFilters }) => {
            // Fetch all jobs matching the main filters with pagination
            const allMatchingJobs = await fetchJobsWithPagination(jobFilters, activeJobSets)

            // Fetch excluded jobs in parallel
            const excludedJobsArrays = await Promise.all(
              excludesJobFilters.map((excludeJobFilters) => fetchJobsWithPagination(excludeJobFilters, activeJobSets)),
            )

            const excludedJobs = excludedJobsArrays.flat()
            const excludedJobIds = new Set(excludedJobs.map((job) => job.jobId))

            // Filter out excluded jobs
            return allMatchingJobs.filter((job) => !excludedJobIds.has(job.jobId))
          }),
        )

        if (isCancelled) return

        // Deduplicate jobs across filter groups and sort by jobId descending
        const uniqueJobs = _.uniqBy(jobsBySelectedItem.flat(), (job) => job.jobId)
        const sortedJobs = _.orderBy(uniqueJobs, (job) => job.jobId, "desc")

        setAllJobs(sortedJobs)
        setIsLoading(false)
      } catch (err) {
        if (isCancelled) return
        setError(err instanceof Error ? err.message : String(err))
        setIsLoading(false)
      }
    }

    fetchAllJobs()

    return () => {
      isCancelled = true
    }
  }, [filtersKey, activeJobSets, enabled, refetchCounter, fetchJobsWithPagination])

  const refetch = useCallback(() => {
    setRefetchCounter((prev) => prev + 1)
  }, [])

  return {
    data: allJobs,
    isLoading,
    error,
    refetch,
  }
}
