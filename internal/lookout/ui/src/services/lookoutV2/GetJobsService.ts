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
  jobs: Job[]
}

export class GetJobsService implements IGetJobsService {
  async getJobs(
    filters: JobFilter[],
    activeJobSets: boolean,
    order: JobOrder,
    skip: number,
    take: number,
    abortSignal?: AbortSignal,
  ): Promise<GetJobsResponse> {
    const response = await fetch("/api/v1/jobs", {
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
      jobs: json.jobs ?? [],
    }
  }
}
