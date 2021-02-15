import {
  LookoutApi,
  LookoutDurationStats,
  LookoutJobInfo,
  LookoutJobSetInfo,
  LookoutQueueInfo,
  LookoutRunInfo
} from '../openapi/lookout'
import { SubmitApi } from '../openapi/armada'

import { reverseMap, secondsToDurationString } from "../utils";

type DurationFromApi = {
  seconds?: number
  nanos?: number
}

export type QueueInfo = {
  queue: string
  jobsQueued: number
  jobsPending: number
  jobsRunning: number
  oldestQueuedJob?: JobRun
  longestRunningJob?: JobRun
  oldestQueuedDuration: string
  longestRunningDuration: string
}

export type JobSet = {
  jobSet: string
  queue: string
  jobsQueued: number
  jobsPending: number
  jobsRunning: number
  jobsSucceeded: number
  jobsFailed: number

  runningStats?: DurationStats
  queuedStats?: DurationStats
}

export type DurationStats = {
  shortest: number
  longest: number
  average: number
  median: number
  q1: number
  q3: number
}

export interface GetJobsRequest {
  queue: string,
  take: number,
  skip: number,
  jobSets: string[],
  newestFirst: boolean,
  jobStates: string[],
  jobId: string,
}

export type Job = {
  jobId: string
  queue: string
  owner: string
  jobSet: string
  priority: number
  submissionTime: string
  cancelledTime?: string
  jobState: string
  runs: Run[]
}

export type Run = {
  k8sId: string
  cluster: string
  node?: string
  succeeded: boolean
  error?: string
  podCreationTime?: string
  podStartTime?: string
  finishTime?: string
}

export type JobRun = Job & {
  podNumber: Number
  runState: string | undefined
}

export type CancelJobsResult = {
  cancelledJobs: Job[]
  failedJobCancellations: FailedJobCancellation[]
}

export type FailedJobCancellation = {
  job: Job
  error: string
}

const JOB_STATE_MAP = new Map<string, string>()
JOB_STATE_MAP.set("QUEUED", "Queued")
JOB_STATE_MAP.set("PENDING", "Pending")
JOB_STATE_MAP.set("RUNNING", "Running")
JOB_STATE_MAP.set("SUCCEEDED", "Succeeded")
JOB_STATE_MAP.set("FAILED", "Failed")
JOB_STATE_MAP.set("CANCELLED", "Cancelled")

const INVERSE_JOB_STATE_MAP = reverseMap(JOB_STATE_MAP)

export const JOB_STATES_FOR_DISPLAY = [
  "Queued",
  "Pending",
  "Running",
  "Succeeded",
  "Failed",
  "Cancelled",
]

export default class JobService {

  lookoutApi: LookoutApi
  submitApi: SubmitApi

  constructor(lookoutAPi: LookoutApi, submitApi: SubmitApi) {
    this.lookoutApi = lookoutAPi
    this.submitApi = submitApi
  }

  async getOverview(): Promise<QueueInfo[]> {
    const queueInfosFromApi = await this.lookoutApi.overview()
    if (!queueInfosFromApi.queues) {
      return []
    }

    return queueInfosFromApi.queues.map(queueInfoToViewModel)
  }

  async getJobSets(queue: string): Promise<JobSet[]> {
    const jobSetsFromApi = await this.lookoutApi.getJobSets({
      body: {
        queue: queue
      }
    })
    if (!jobSetsFromApi.jobSetInfos) {
      return []
    }

    return jobSetsFromApi.jobSetInfos.map(jobSetToViewModel)
  }


  async getJobsInQueue(getJobsRequest: GetJobsRequest): Promise<JobRun[]> {
    const jobStatesForApi = getJobsRequest.jobStates.map(getJobStateForApi)
    const jobSetsForApi = getJobsRequest.jobSets.map(escapeBackslashes)
    try {
      const response = await this.lookoutApi.getJobs({
        body: {
          queue: getJobsRequest.queue,
          take: getJobsRequest.take,
          skip: getJobsRequest.skip,
          jobSetIds: jobSetsForApi,
          newestFirst: getJobsRequest.newestFirst,
          jobStates: jobStatesForApi,
          jobId: getJobsRequest.jobId,
        }
      });
      if (response.jobInfos) {
        return response.jobInfos.flatMap(jobInfoToJobRunViewModel)
      }
    } catch (e) {
      console.error(await e.json())
    }
    return []
  }

  async cancelJobs(jobs: Job[]): Promise<CancelJobsResult> {
    const result: CancelJobsResult = { cancelledJobs: [], failedJobCancellations: [] }
    for (let job of jobs) {
      try {
        const apiResult = await this.submitApi.cancelJobs({
          body: {
            jobId: job.jobId,
          },
        })

        if (!apiResult.cancelledIds ||
          apiResult.cancelledIds.length !== 1 ||
          apiResult.cancelledIds[0] !== job.jobId) {
          result.failedJobCancellations.push({ job: job, error: "No job was cancelled" })
        } else {
          result.cancelledJobs.push(job)
        }
      } catch (e) {
        console.error(e)
        result.failedJobCancellations.push({ job: job, error: e.toString() })
      }
    }
    return result
  }
}

function escapeBackslashes(str: string) {
  return str.replaceAll("\\", "\\\\")
}

function queueInfoToViewModel(queueInfo: LookoutQueueInfo): QueueInfo {
  let oldestQueuedJob: JobRun | undefined
  let oldestQueuedDuration = "-"
  if (queueInfo.oldestQueuedJob) {
    oldestQueuedJob = jobInfoToJobRunViewModel(queueInfo.oldestQueuedJob)[0]
  }
  if (queueInfo.oldestQueuedDuration) {
    oldestQueuedDuration = getDurationString(queueInfo.oldestQueuedDuration)
  }

  let longestRunningJob: JobRun | undefined
  let longestRunningJobDuration = "-"
  if (queueInfo.longestRunningJob) {
    longestRunningJob = jobInfoToJobRunViewModel(queueInfo.longestRunningJob)[0]
  }
  if (queueInfo.longestRunningDuration) {
    longestRunningJobDuration = getDurationString(queueInfo.longestRunningDuration)
  }

  return {
    queue: queueInfo.queue ?? "Unknown queue",
    jobsQueued: queueInfo.jobsQueued ?? 0,
    jobsPending: queueInfo.jobsPending ?? 0,
    jobsRunning: queueInfo.jobsRunning ?? 0,
    oldestQueuedJob: oldestQueuedJob,
    longestRunningJob: longestRunningJob,
    oldestQueuedDuration: oldestQueuedDuration,
    longestRunningDuration: longestRunningJobDuration,
  }
}

function jobSetToViewModel(jobSet: LookoutJobSetInfo): JobSet {
  return {
    jobSet: jobSet.jobSet ?? "Unknown job set",
    queue: jobSet.queue ?? "Unknown queue",
    jobsQueued: jobSet.jobsQueued ?? 0,
    jobsPending: jobSet.jobsPending ?? 0,
    jobsRunning: jobSet.jobsRunning ?? 0,
    jobsSucceeded: jobSet.jobsSucceeded ?? 0,
    jobsFailed: jobSet.jobsFailed ?? 0,
    runningStats: durationStatsToViewModel(jobSet.runningStats),
    queuedStats: durationStatsToViewModel(jobSet.queuedStats),
  }
}

function durationStatsToViewModel(durationStats?: LookoutDurationStats): DurationStats | undefined {
  if (!(
    durationStats &&
    durationStats.shortest &&
    durationStats.longest &&
    durationStats.average &&
    durationStats.median &&
    durationStats.q1 &&
    durationStats.q3
  )) {
    return undefined
  }

  return {
    shortest: getDurationSeconds(durationStats.shortest),
    longest: getDurationSeconds(durationStats.longest),
    average: getDurationSeconds(durationStats.average),
    median: getDurationSeconds(durationStats.median),
    q1: getDurationSeconds(durationStats.q1),
    q3: getDurationSeconds(durationStats.q3),
  }
}

function getDurationString(durationFromApi: any): string {
  const totalSeconds = getDurationSeconds(durationFromApi)
  return secondsToDurationString(totalSeconds)
}

function getDurationSeconds(durationFromApi: any): number {
  durationFromApi = durationFromApi as DurationFromApi
  let totalSeconds = 0

  if (durationFromApi.seconds) {
    totalSeconds += durationFromApi.seconds
  }

  if (durationFromApi.nanos) {
    totalSeconds += durationFromApi.nanos / 1e9
  }

  return totalSeconds
}

function jobInfoToJobRunViewModel(jobInfo: LookoutJobInfo): JobRun[] {
  let job = jobInfoToViewModel(jobInfo)
  return jobInfo.runs?.length ?
    jobInfo.runs.map(r => ({...job, podNumber: r.podNumber ?? 0, runState: JOB_STATE_MAP.get(r.runState ?? "") })) :
    [{...job, podNumber: 0, runState: ""}];
}

function jobInfoToViewModel(jobInfo: LookoutJobInfo): Job {
  const jobId = jobInfo.job?.id ?? "-"
  const queue = jobInfo.job?.queue ?? "-"
  const owner = jobInfo.job?.owner ?? "-"
  const jobSet = jobInfo.job?.jobSetId ?? "-"
  const priority = jobInfo.job?.priority ?? 0
  const submissionTime = dateToString(jobInfo.job?.created ?? new Date())
  const cancelledTime = jobInfo.cancelled ? dateToString(jobInfo.cancelled) : undefined
  const jobState = JOB_STATE_MAP.get(jobInfo.jobState ?? "") ?? "Unknown"
  const runs = getRuns(jobInfo)

  return {
    jobId: jobId,
    queue: queue,
    owner: owner,
    jobSet: jobSet,
    priority: priority,
    submissionTime: submissionTime,
    cancelledTime: cancelledTime,
    jobState: jobState,
    runs: runs,
  }
}

function dateToString(date: Date): string {
  return date.toLocaleString("en-GB", {
    timeZone: "UTC",
    timeZoneName: "short",
  })
}

function getRuns(jobInfo: LookoutJobInfo): Run[] {
  if (!jobInfo.runs || jobInfo.runs.length === 0) {
    return []
  }

  return jobInfo.runs.map(runInfoToViewModel)
}

function runInfoToViewModel(run: LookoutRunInfo): Run {
  return {
    k8sId: run.k8sId ?? "Unknown Kubernetes id",
    cluster: run.cluster ?? "Unknown cluster",
    node: run.node,
    succeeded: run.succeeded ?? false,
    error: run.error,
    podCreationTime: run.created ? dateToString(run.created) : undefined,
    podStartTime: run.started ? dateToString(run.started) : undefined,
    finishTime: run.finished ? dateToString(run.finished) : undefined,
  }
}

function getJobStateForApi(displayedJobState: string): string {
  const jobState = INVERSE_JOB_STATE_MAP.get(displayedJobState)
  if (!jobState) {
    throw new Error(`Unrecognized job state: "${displayedJobState}"`)
  }
  return jobState
}
