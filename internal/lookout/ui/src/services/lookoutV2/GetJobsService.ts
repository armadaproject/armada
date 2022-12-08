import { Job, JobFilter, JobOrder } from "models/lookoutV2Models"

export default interface GetJobsService {
  getJobs(
    filters: JobFilter[],
    order: JobOrder,
    skip: number,
    take: number,
    signal: AbortSignal | undefined,
  ): Promise<GetJobsResponse>
}

export type GetJobsResponse = {
  totalJobs: number
  jobs: Job[]
}
