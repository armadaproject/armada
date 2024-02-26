import { JobFilter, JobGroup, JobOrder } from "models/lookoutV2Models"

export interface IGroupJobsService {
  groupJobs(
    filters: JobFilter[],
    activeJobSets: boolean,
    order: JobOrder,
    groupedField: GroupedField,
    aggregates: string[],
    skip: number,
    take: number,
    abortSignal?: AbortSignal,
  ): Promise<GroupJobsResponse>
}

export type GroupedField = {
  field: string
  isAnnotation: boolean
}

export type GroupJobsResponse = {
  groups: JobGroup[]
}

export class GroupJobsService implements IGroupJobsService {
  private backend: string | undefined

  constructor(backend: string | undefined) {
    this.backend = backend
  }

  async groupJobs(
    filters: JobFilter[],
    activeJobSets: boolean,
    order: JobOrder,
    groupedField: GroupedField,
    aggregates: string[],
    skip: number,
    take: number,
    abortSignal?: AbortSignal,
  ): Promise<GroupJobsResponse> {
    let path = "/api/v1/jobGroups"
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
        groupedField,
        aggregates,
        skip,
        take,
      }),
      signal: abortSignal,
    })

    const json = await response.json()
    return {
      groups: json.groups ?? [],
    }
  }
}
