import { LookoutApi, LookoutJobInfo } from '../openapi'
import { reverseMap } from "../utils";

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

  getOverview() {
    return this.api.overview()
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
