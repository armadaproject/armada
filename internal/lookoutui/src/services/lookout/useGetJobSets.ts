import { useQuery } from "@tanstack/react-query"

import { JobSet, JobSetsOrderByColumn, JobState, Match } from "../../models/lookoutModels"

import { useGroupJobs } from "./useGroupJobs"

export interface UseGetJobSetsParams {
  queue: string
  activeOnly: boolean
  orderByColumn: JobSetsOrderByColumn
  orderByDesc: boolean
  autoRefresh: boolean
  autoRefreshMs: number | undefined
}

export const useGetJobSets = ({
  queue,
  activeOnly,
  orderByColumn,
  orderByDesc,
  autoRefresh,
  autoRefreshMs,
}: UseGetJobSetsParams) => {
  const groupJobs = useGroupJobs()

  return useQuery<JobSet[], string>({
    queryKey: ["getJobSets", queue, activeOnly, orderByColumn, orderByDesc],
    queryFn: async ({ signal }) => {
      const response = await groupJobs(
        [{ isAnnotation: false, field: "queue", value: queue, match: Match.Exact }],
        activeOnly,
        { field: orderByColumn, direction: orderByDesc ? "DESC" : "ASC" },
        { field: "jobSet", isAnnotation: false },
        ["state", "submitted"],
        0,
        0,
        signal,
      )

      return response.groups.map((group) => {
        const state = group.aggregates.state as Record<string, number>
        return {
          jobSetId: group.name,
          queue,
          jobsQueued: state[JobState.Queued] || 0,
          jobsPending: state[JobState.Pending] || 0,
          jobsRunning: state[JobState.Running] || 0,
          jobsSucceeded: state[JobState.Succeeded] || 0,
          jobsFailed: state[JobState.Failed] || 0,
          jobsCancelled: state[JobState.Cancelled] || 0,
          latestSubmissionTime: group.aggregates.submitted as string,
        }
      })
    },
    enabled: Boolean(queue),
    refetchInterval: autoRefresh && autoRefreshMs !== undefined ? autoRefreshMs : false,
  })
}
