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

export const JOB_SETS_ORDER_BY_COLUMNS = ["submitted", "jobSet"] as const

export type JobSetsOrderByColumn = (typeof JOB_SETS_ORDER_BY_COLUMNS)[number]

export const isJobSetsOrderByColumn = (v: any): v is JobSetsOrderByColumn =>
  typeof v === "string" && ([...JOB_SETS_ORDER_BY_COLUMNS] as string[]).includes(v)

export interface GetJobSetsRequest {
  queue: string
  orderByColumn: JobSetsOrderByColumn
  orderByDesc: boolean
  activeOnly: boolean
}
