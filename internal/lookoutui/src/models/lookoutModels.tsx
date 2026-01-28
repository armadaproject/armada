import { ReactElement } from "react"

import { SvgIconProps } from "@mui/material"

import {
  FaHourglassHalf,
  FaSpinner,
  FaPlayCircle,
  FaCheckCircle,
  FaTimesCircle,
  FaBan,
  FaExchangeAlt,
  FaFileContract,
  FaHand,
} from "../components/fontAwesomeIcons"
import { CustomPaletteColorToken } from "../theme"

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
  Rejected = "REJECTED",
}

export const jobStateColors: Record<JobState, CustomPaletteColorToken> = {
  [JobState.Queued]: "statusQueued",
  [JobState.Pending]: "statusPending",
  [JobState.Running]: "statusRunning",
  [JobState.Succeeded]: "statusSucceeded",
  [JobState.Failed]: "statusFailed",
  [JobState.Cancelled]: "statusCancelled",
  [JobState.Preempted]: "statusPreempted",
  [JobState.Leased]: "statusLeased",
  [JobState.Rejected]: "statusRejected",
}

export const jobStateIcons: Record<JobState, (svgIconProps: SvgIconProps) => ReactElement> = {
  [JobState.Queued]: FaHourglassHalf,
  [JobState.Pending]: FaSpinner,
  [JobState.Running]: FaPlayCircle,
  [JobState.Succeeded]: FaCheckCircle,
  [JobState.Failed]: FaTimesCircle,
  [JobState.Cancelled]: FaBan,
  [JobState.Preempted]: FaExchangeAlt,
  [JobState.Leased]: FaFileContract,
  [JobState.Rejected]: FaHand,
}

export const jobStateDisplayNames: Record<JobState, string> = {
  [JobState.Leased]: "Leased",
  [JobState.Queued]: "Queued",
  [JobState.Pending]: "Pending",
  [JobState.Running]: "Running",
  [JobState.Succeeded]: "Succeeded",
  [JobState.Failed]: "Failed",
  [JobState.Cancelled]: "Cancelled",
  [JobState.Preempted]: "Preempted",
  [JobState.Rejected]: "Rejected",
}

const terminatedJobStates = new Set([
  JobState.Succeeded,
  JobState.Failed,
  JobState.Cancelled,
  JobState.Preempted,
  JobState.Rejected,
])
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
  RunCancelled = "RUN_CANCELLED",
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
  [JobRunState.RunCancelled]: { displayName: "Cancelled" },
}

export type JobId = string

export type Job = {
  jobId: JobId
  queue: string
  owner: string
  namespace: string
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
  cancelUser?: string
  node?: string
  cluster?: string
  pool?: string
  exitCode?: number
  runtimeSeconds?: number
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
  ingressAddresses?: Record<string | number, string>
  pool?: string
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

export interface JobFiltersWithExcludes {
  jobFilters: JobFilter[]
  excludesJobFilters: JobFilter[][]
}

export const aggregateTypes = ["latest", "earliest", "average"] as const

/**
 * Represents the different ways to aggregate the latestTransitionTime field.
 * - latest: Shows the most recent transaction time (MAX in SQL)
 * - earliest: Shows the oldest transaction time (MIN in SQL)
 * - average: Shows the average transaction time (AVG in SQL)
 */
export type AggregateType = (typeof aggregateTypes)[number]

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

export interface JobSet {
  queue: string
  jobSetId: string
}
