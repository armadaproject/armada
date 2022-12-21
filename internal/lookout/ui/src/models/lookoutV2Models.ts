// Values must match the server-side states
export enum JobState {
  Queued = "QUEUED",
  Pending = "PENDING",
  Running = "RUNNING",
  Succeeded = "SUCCEEDED",
  Failed = "FAILED",
  Cancelled = "CANCELLED",
}

export const jobStateDisplayInfo: Record<JobState, ColoredState> = {
  [JobState.Queued]: { displayName: "Queued", color: "#ffff00" },
  [JobState.Pending]: { displayName: "Pending", color: "#ff9900" },
  [JobState.Running]: { displayName: "Running", color: "#00ff00" },
  [JobState.Succeeded]: { displayName: "Succeeded", color: "#0000ff" },
  [JobState.Failed]: { displayName: "Failed", color: "#ff0000" },
  [JobState.Cancelled]: { displayName: "Cancelled", color: "#999999" },
}

const terminatedJobStates = new Set([JobState.Succeeded, JobState.Failed, JobState.Cancelled])
export const isTerminatedJobState = (state: JobState) => terminatedJobStates.has(state)

export enum JobRunState {
  RunPending = "RUN_PENDING",
  RunRunning = "RUN_RUNNING",
  RunSucceeded = "RUN_SUCCEEDED",
  RunFailed = "RUN_FAILED",
  RunTerminated = "RUN_TERMINATED",
  RunPreempted = "RUN_PREEMPTED",
  RunUnableToSchedule = "RUN_UNABLE_TO_SCHEDULE",
  RunLeaseReturned = "RUN_LEASE_RETURNED",
  RunLeaseExpired = "RUN_LEASE_EXPIRED",
  RunMaxRunsExceeded = "RUN_MAX_RUNS_EXCEEDED",
}

export const jobRunStateDisplayInfo: Record<JobRunState, { displayName: string }> = {
  [JobRunState.RunPending]: { displayName: "Pending" },
  [JobRunState.RunRunning]: { displayName: "Running" },
  [JobRunState.RunSucceeded]: { displayName: "Succeeded" },
  [JobRunState.RunFailed]: { displayName: "Failed" },
  [JobRunState.RunTerminated]: { displayName: "Terminated" },
  [JobRunState.RunUnableToSchedule]: { displayName: "Unable To Schedule" },
  [JobRunState.RunLeaseReturned]: { displayName: "Lease Returned" },
  [JobRunState.RunPreempted]: { displayName: "Preempted" },
  [JobRunState.RunLeaseExpired]: { displayName: "Lease Expired" },
  [JobRunState.RunMaxRunsExceeded]: { displayName: "Max Runs Expired" },
}

type ColoredState = {
  displayName: string
  color: string
}

export type JobId = string

export type Job = {
  jobId: JobId
  queue: string
  owner: string
  jobSet: string
  state: JobState
  cpu: number
  memory: number
  ephemeralStorage: number
  gpu: number
  priority: number
  submitted: string
  annotations: Record<string, string>
  runs: JobRun[]
  lastActiveRunId?: string
  lastTransitionTime: string
}

export type JobKey = keyof Job

export type JobRun = {
  runId: string
  jobId: string
  cluster: string
  node?: string
  pending: string
  started?: string
  finished?: string
  jobRunState: JobRunState
  error?: string
  exitCode?: number
}

export enum Match {
  Exact = "exact",
  StartsWith = "startsWith",
  GreaterThan = "greater",
  LessThan = "less",
  GreaterThanOrEqual = "greaterOrEqual",
  LessThanOrEqual = "lessOrEqual",
  AnyOf = "anyOf",
}

export type JobFilter = {
  isAnnotation?: boolean
  field: string
  value: string | number | string[] | number[]
  match: Match
}

export type JobGroup = {
  name: string
  count: number
  aggregates: Record<string, string | number>
}

export type SortDirection = "ASC" | "DESC"

export type JobOrder = {
  field: string
  direction: SortDirection
}
