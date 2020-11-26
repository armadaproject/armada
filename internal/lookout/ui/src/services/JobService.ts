import { LookoutApi, LookoutJobInfo } from '../openapi'
import { reverseMap } from "../utils";

export type JobInfoViewModel = {
  jobId: string
  queue: string
  owner: string
  jobSet: string
  submissionTime: string
  jobState: JobStateViewModel
}

export enum JobStateViewModel {
  Queued = "Queued",
  Pending = "Pending",
  Running = "Running",
  Succeeded = "Succeeded",
  Failed = "Failed",
  Cancelled = "Cancelled",
  Unknown = "Unknown",
}

enum JobState {
  Queued = "QUEUED",
  Pending = "PENDING",
  Running = "RUNNING",
  Succeeded = "SUCCEEDED",
  Failed = "FAILED",
  Cancelled = "CANCELLED",
}

const JOB_STATE_VIEW_MODEL_MAP = new Map<JobState, JobStateViewModel>()
JOB_STATE_VIEW_MODEL_MAP.set(JobState.Queued, JobStateViewModel.Queued)
JOB_STATE_VIEW_MODEL_MAP.set(JobState.Pending, JobStateViewModel.Pending)
JOB_STATE_VIEW_MODEL_MAP.set(JobState.Running, JobStateViewModel.Running)
JOB_STATE_VIEW_MODEL_MAP.set(JobState.Succeeded, JobStateViewModel.Succeeded)
JOB_STATE_VIEW_MODEL_MAP.set(JobState.Failed, JobStateViewModel.Failed)
JOB_STATE_VIEW_MODEL_MAP.set(JobState.Cancelled, JobStateViewModel.Cancelled)

const VIEW_MODEL_JOB_STATE_MAP = reverseMap(JOB_STATE_VIEW_MODEL_MAP)

export const VALID_JOB_STATE_VIEW_MODELS = getValidJobStateViewModels(Object.values(JobStateViewModel))

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
    jobStates: JobStateViewModel[],
  ): Promise<JobInfoViewModel[]> {
    const jobStatesForApi = jobStates.map(jobStateViewModelToJobState)
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

function getValidJobStateViewModels(allJobStateViewModels: JobStateViewModel[]): JobStateViewModel[] {
  const copy = allJobStateViewModels.slice()
  const index = copy.indexOf(JobStateViewModel.Unknown)
  if (index < 0) {
    throw new Error("'Unknown' job state view model not found")
  }
  copy.splice(index, 1)
  return copy
}

function jobStateViewModelToJobState(jobStateViewModel: JobStateViewModel): JobState {
  const jobState = VIEW_MODEL_JOB_STATE_MAP.get(jobStateViewModel)
  if (!jobState) {
    throw new Error(`Unrecognized job state: "${jobStateViewModel}"`)
  }
  return jobState
}

function jobInfoToViewModel(jobInfo: LookoutJobInfo): JobInfoViewModel {
  const jobId = jobInfo.job?.id ?? "-"
  const queue = jobInfo.job?.queue ?? "-"
  const owner = jobInfo.job?.owner ?? "-"
  const jobSet = jobInfo.job?.jobSetId ?? "-"
  const submissionTime = (jobInfo.job?.created ?? new Date()).toLocaleString()
  const jobState = JOB_STATE_VIEW_MODEL_MAP.get(jobInfo.jobState as JobState) ?? JobStateViewModel.Unknown

  return {
    jobId: jobId,
    queue: queue,
    owner: owner,
    jobSet: jobSet,
    submissionTime: submissionTime,
    jobState: jobState,
  }
}
