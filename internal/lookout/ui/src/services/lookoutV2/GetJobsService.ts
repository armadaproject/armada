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
  private backend: string | undefined

  constructor(backend: string | undefined) {
    this.backend = backend
  }

  async getJobs(
    filters: JobFilter[],
    activeJobSets: boolean,
    order: JobOrder,
    skip: number,
    take: number,
    abortSignal?: AbortSignal,
  ): Promise<GetJobsResponse> {
    let path = "/api/v1/jobs"
    if (this.backend) {
      path += "?" + new URLSearchParams({ backend: this.backend })
    }
    const response = await fetch(path, {
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
