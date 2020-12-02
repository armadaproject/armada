import { LookoutApi, LookoutJobInfo, LookoutQueueInfo } from '../openapi'
import { reverseMap } from "../utils";

export type QueueInfo = {
  queue: string
  jobsQueued: number
  jobsPending: number
  jobsRunning: number
}

export type JobInfoViewModel = {
  jobId: string
  queue: string
  owner: string
  jobSet: string
  submissionTime: string
  jobState: string
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

  api: LookoutApi;

  constructor(lookoutAPi: LookoutApi) {
    this.api = lookoutAPi
  }

  async getOverview(): Promise<QueueInfo[]> {
    const queueInfosFromApi = await this.api.overview()
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
  ): Promise<JobInfoViewModel[]> {
    const jobStatesForApi = jobStates.map(getJobStateForApi)
    try {
      const response = await this.api.getJobsInQueue({
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
}

function queueInfoToViewModel(queueInfo: LookoutQueueInfo): QueueInfo {
  return {
    queue: queueInfo.queue ?? "Unknown queue",
    jobsQueued: queueInfo.jobsQueued ?? 0,
    jobsPending: queueInfo.jobsPending ?? 0,
    jobsRunning: queueInfo.jobsRunning ?? 0,
  }
}

function getJobStateForApi(displayedJobState: string): string {
  const jobState = INVERSE_JOB_STATE_MAP.get(displayedJobState)
  if (!jobState) {
    throw new Error(`Unrecognized job state: "${displayedJobState}"`)
  }
  return jobState
}

function jobInfoToViewModel(jobInfo: LookoutJobInfo): JobInfoViewModel {
  const jobId = jobInfo.job?.id ?? "-"
  const queue = jobInfo.job?.queue ?? "-"
  const owner = jobInfo.job?.owner ?? "-"
  const jobSet = jobInfo.job?.jobSetId ?? "-"
  const submissionTime = (jobInfo.job?.created ?? new Date()).toLocaleString()
  const jobState = JOB_STATE_MAP.get(jobInfo.jobState ?? "") ?? "Unknown"

  return {
    jobId: jobId,
    queue: queue,
    owner: owner,
    jobSet: jobSet,
    submissionTime: submissionTime,
    jobState: jobState,
  }
}
