export const JobStates: Record<string, ColoredState> = {
  Queued: { name: "Queued", color: "#ffff00" },
  Pending: { name: "Pending", color: "#ff9900" },
  Running: { name: "Running", color: "#00ff00" },
  Succeeded: { name: "Succeeded", color: "#0000ff" },
  Failed: { name: "Failed", color: "#ff0000" },
  Cancelled: { name: "Cancelled", color: "#999999" },
}

export const JobRunStates: Record<string, ColoredState> = {
  RunPending: { name: "Run Pending", color: "#ff9900" },
  RunRunning: { name: "Run Running", color: "#00ff00" },
  RunSucceeded: { name: "Run Succeeded", color: "#0000ff" },
  RunFailed: { name: "Run Failed", color: "#ff0000" },
  RunTerminated: { name: "Terminated", color: "#ffffff" },
  RunPreempted: { name: "Preempted", color: "#ffff00" },
  RunUnableToSchedule: { name: "Unable to Schedule", color: "#ff0000" },
  RunLeaseReturned: { name: "Lease Returned", color: "#ff0000" },
  RunLeaseExpired: { name: "Lease Expired", color: "#ff0000" },
  RunMaxRunsExceeded: { name: "Max Runs Exceeded", color: "#ff0000" },
}

type ColoredState = {
  name: string
  color: string
}

export type JobId = string

export type Job = {
  jobId: JobId
  queue: string
  owner: string
  jobSet: string
  state: string
  cpu: number
  memory: string
  ephemeralStorage: string
  gpu: number
  priority: number
  priorityClass: string
  submitted: string
  cancelled?: string
  timeInState: string
  duplicate: boolean
  annotations: Record<string, string>
  runs: JobRun[]
  lastActiveRunId?: string
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
  jobRunState: string
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
