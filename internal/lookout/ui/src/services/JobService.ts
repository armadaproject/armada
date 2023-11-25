export type JobSet = {
  jobSetId: string
  queue: string
  jobsQueued: number
  jobsPending: number
  jobsRunning: number
  jobsSucceeded: number
  jobsFailed: number
  jobsCancelled: number
  latestSubmissionTime: string
}

export interface GetJobSetsRequest {
  queue: string
  newestFirst: boolean
  activeOnly: boolean
}
