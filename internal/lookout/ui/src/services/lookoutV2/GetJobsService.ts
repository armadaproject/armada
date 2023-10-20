import { Job, JobFilter, JobOrder } from "models/lookoutV2Models"

export interface IGetJobsService {
  getJobs(
    filters: JobFilter[],
    activeJobSets: boolean,
    order: JobOrder,
    skip: number,
    take: number,
    abortSignal?: AbortSignal,
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
    activeJobSets: boolean,
    order: JobOrder,
    skip: number,
    take: number,
    abortSignal?: AbortSignal,
  ): Promise<GetJobsResponse> {
    const response = await fetch(this.apiBase + "/api/v1/jobs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        filters,
        activeJobSets,
        order,
        skip,
        take,
      }),
      signal: abortSignal,
    })

    const json = await response.json()
    return {
      count: json.count ?? 0,
      jobs: json.jobs ?? [],
    }
  }
}
