import { LookoutApi, LookoutJobInfo, LookoutJobSetInfo, LookoutQueueInfo, LookoutRunInfo } from '../openapi/lookout'
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

export type JobSet = {
  jobSet: string
  queue: string
  jobsQueued: number
  jobsPending: number
  jobsRunning: number
  jobsSucceeded: number
  jobsFailed: number
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

  async getJobsInQueue(getJobsRequest: GetJobsRequest): Promise<Job[]> {
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
        console.log("HIT")
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

function jobSetToViewModel(jobSet: LookoutJobSetInfo): JobSet {
  return {
    jobSet: jobSet.jobSet ?? "Unknown job set",
    queue: jobSet.queue ?? "Unknown queue",
    jobsQueued: jobSet.jobsQueued ?? 0,
    jobsPending: jobSet.jobsPending ?? 0,
    jobsRunning: jobSet.jobsRunning ?? 0,
    jobsSucceeded: jobSet.jobsSucceeded ?? 0,
    jobsFailed: jobSet.jobsFailed ?? 0,
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

const TEST_RUNS = [
  {
    k8sId: "0001",
    cluster: "a",
    node: "node",
    succeeded: false,
    error: "With this utility you generate a 16 character output based on your input of numbers and upper and lower case letters.  Random strings can be unique. Used in computing, a random string generator can also be called a random character string generator. This is an important tool if you want to generate a unique set of strings. The utility generates a sequence that lacks a pattern and is random.",
    podCreationTime: "a time",
  },
  {
    k8sId: "0002",
    cluster: "b",
    node: "node",
    succeeded: false,
    podCreationTime: "another time",
  },
]

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

  runs.unshift(...TEST_RUNS)

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
