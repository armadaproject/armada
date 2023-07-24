// Values must match the server-side states
export enum JobState {
  Queued = "QUEUED",
  Leased = "LEASED",
  Pending = "PENDING",
  Running = "RUNNING",
  Succeeded = "SUCCEEDED",
  Failed = "FAILED",
  Cancelled = "CANCELLED",
  Preempted = "PREEMPTED",
}

export const jobStateDisplayInfo: Record<JobState, ColoredState> = {
  [JobState.Leased]: { displayName: "Leased", color: "#f5c056" },
  [JobState.Queued]: { displayName: "Queued", color: "#ffff00" },
  [JobState.Pending]: { displayName: "Pending", color: "#ff9900" },
  [JobState.Running]: { displayName: "Running", color: "#00ff00" },
  [JobState.Succeeded]: { displayName: "Succeeded", color: "#0000ff" },
  [JobState.Failed]: { displayName: "Failed", color: "#ff0000" },
  [JobState.Cancelled]: { displayName: "Cancelled", color: "#999999" },
  [JobState.Preempted]: { displayName: "Preempted", color: "#f8bbd0" },
}

const terminatedJobStates = new Set([JobState.Succeeded, JobState.Failed, JobState.Cancelled, JobState.Preempted])
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
  RunLeased = "RUN_LEASED",
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
  [JobRunState.RunMaxRunsExceeded]: { displayName: "Max Runs Exceeded" },
  [JobRunState.RunLeased]: { displayName: "Leased" },
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
  priorityClass: string
  submitted: string
  annotations: Record<string, string>
  runs: JobRun[]
  lastActiveRunId?: string
  lastTransitionTime: string
  cancelReason?: string
}

export type JobKey = keyof Job

export type JobRun = {
  runId: string
  jobId: string
  cluster: string
  node?: string
  leased?: string
  pending?: string
  started?: string
  finished?: string
  jobRunState: JobRunState
  exitCode?: number
}

export enum Match {
  Exact = "exact",
  StartsWith = "startsWith",
  Contains = "contains",
  GreaterThan = "greaterThan",
  LessThan = "lessThan",
  GreaterThanOrEqual = "greaterThanOrEqualTo",
  LessThanOrEqual = "lessThanOrEqualTo",
  AnyOf = "anyOf",
  Exists = "exists",
}

export const MATCH_DISPLAY_STRINGS: Record<Match, string> = {
  [Match.Exact]: "Exact",
  [Match.StartsWith]: "Starts with",
  [Match.Contains]: "Contains",
  [Match.GreaterThan]: "Greater than",
  [Match.LessThan]: "Less than",
  [Match.GreaterThanOrEqual]: "Greater than or equal to",
  [Match.LessThanOrEqual]: "Less than or equal to",
  [Match.AnyOf]: "Any of",
  [Match.Exists]: "Exists",
}

export const isValidMatch = (match: string): match is Match => (Object.values(Match) as string[]).includes(match)

export type JobFilter = {
  isAnnotation?: boolean
  field: string
  value: string | number | string[] | number[]
  match: Match
}

export type JobGroup = {
  name: string
  count: number
  aggregates: Record<string, string | number | Record<string, number>>
}

export type SortDirection = "ASC" | "DESC"

export type JobOrder = {
  field: string
  direction: SortDirection
}
