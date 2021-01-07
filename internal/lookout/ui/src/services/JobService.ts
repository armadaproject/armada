import { LookoutApi, LookoutJobInfo, LookoutQueueInfo } from '../openapi/lookout'
import { SubmitApi } from '../openapi/armada'
import { reverseMap } from "../utils";

export type QueueInfo = {
  queue: string
  jobsQueued: number
  jobsPending: number
  jobsRunning: number
  oldestQueuedJob?: Job
  longestRunningJob?: Job
  oldestQueuedDuration: string
  longestRunningDuration: string
}

export type Job = {
  jobId: string
  queue: string
  owner: string
  jobSet: string
  submissionTime: string
  jobState: string
  cluster?: string
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

  async getJobsInQueue(
    queue: string,
    take: number,
    skip: number,
    jobSets: string[],
    newestFirst: boolean,
    jobStates: string[],
  ): Promise<Job[]> {
    const jobStatesForApi = jobStates.map(getJobStateForApi)
    try {
      const response = await this.lookoutApi.getJobsInQueue({
        body: {
          queue: queue,
          take: take,
          skip: skip,
          jobSetIds: jobSets,
          newestFirst: newestFirst,
          jobStates: jobStatesForApi,
        }
      });
      if (response.jobInfos) {
        return response.jobInfos.map(jobInfoToViewModel)
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

function queueInfoToViewModel(queueInfo: LookoutQueueInfo): QueueInfo {
  let oldestQueuedJob: Job | undefined
  let oldestQueuedDuration = "-"
  if (queueInfo.oldestQueuedJob) {
    oldestQueuedJob = jobInfoToViewModel(queueInfo.oldestQueuedJob)
  }
  if (queueInfo.oldestQueuedDuration) {
    oldestQueuedDuration = getDurationString(queueInfo.oldestQueuedDuration)
  }

  let longestRunningJob: Job | undefined
  let longestRunningJobDuration = "-"
  if (queueInfo.longestRunningJob) {
    longestRunningJob = jobInfoToViewModel(queueInfo.longestRunningJob)
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

function getDurationString(durationFromApi: any): string {
  durationFromApi = durationFromApi as { seconds: number }
  const totalSeconds = durationFromApi.seconds
  const days = Math.floor(totalSeconds / (24 * 3600))
  const hours = Math.floor(totalSeconds / 3600) % 24
  const minutes = Math.floor((totalSeconds % 3600) / 60)
  const seconds = totalSeconds % 60

  const segments: string[] = []

  if (days > 0) {
    segments.push(`${days}d`)
  }
  if (hours > 0) {
    segments.push(`${hours}h`)
  }
  if (minutes > 0) {
    segments.push(`${minutes}m`)
  }
  if (seconds > 0) {
    segments.push(`${seconds}s`)
  }
  if (segments.length === 0) {
    return "Just now"
  }

  return segments.join(" ")
}

function jobInfoToViewModel(jobInfo: LookoutJobInfo): Job {
  const jobId = jobInfo.job?.id ?? "-"
  const queue = jobInfo.job?.queue ?? "-"
  const owner = jobInfo.job?.owner ?? "-"
  const jobSet = jobInfo.job?.jobSetId ?? "-"
  const submissionTime = (jobInfo.job?.created ?? new Date()).toLocaleString()
  const jobState = JOB_STATE_MAP.get(jobInfo.jobState ?? "") ?? "Unknown"
  const cluster = getCurrentCluster(jobInfo)

  return {
    jobId: jobId,
    queue: queue,
    owner: owner,
    jobSet: jobSet,
    submissionTime: submissionTime,
    jobState: jobState,
    cluster: cluster,
  }
}

function getCurrentCluster(jobInfo: LookoutJobInfo): string | undefined {
  if (!jobInfo.runs || jobInfo.runs.length === 0) {
    return undefined
  }

  const lastRun = jobInfo.runs[jobInfo.runs.length - 1]
  return lastRun.cluster
}

function getJobStateForApi(displayedJobState: string): string {
  const jobState = INVERSE_JOB_STATE_MAP.get(displayedJobState)
  if (!jobState) {
    throw new Error(`Unrecognized job state: "${displayedJobState}"`)
  }
  return jobState
}
