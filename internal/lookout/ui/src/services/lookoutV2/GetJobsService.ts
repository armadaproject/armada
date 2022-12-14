import { Job, JobFilter, JobOrder } from "models/lookoutV2Models"

export interface IGetJobsService {
  getJobs(
    filters: JobFilter[],
    order: JobOrder,
    skip: number,
    take: number,
    signal: AbortSignal | undefined,
  ): Promise<GetJobsResponse>
}

export type GetJobsResponse = {
  count: number // Total number of jobs matching the filter (beyond those returned)
  jobs: Job[]
}

export class GetJobsService implements IGetJobsService {
  constructor(private apiBase: string) {}

  async getJobs(
    filters: JobFilter[],
    order: JobOrder,
    skip: number,
    take: number,
    signal: AbortSignal | undefined,
  ): Promise<GetJobsResponse> {
    const response = await fetch(this.apiBase + "/api/v1/jobs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        filters,
        order,
        skip,
        take,
        signal,
      }),
    })

    return await response.json()
  }
}
