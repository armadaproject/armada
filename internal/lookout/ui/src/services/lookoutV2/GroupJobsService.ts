import { JobFilter, JobGroup, JobOrder } from "models/lookoutV2Models"

export interface IGroupJobsService {
  groupJobs(
    filters: JobFilter[],
    order: JobOrder,
    groupedField: GroupedField,
    aggregates: string[],
    skip: number,
    take: number,
    abortSignal: AbortSignal | undefined,
  ): Promise<GroupJobsResponse>
}

export type GroupedField = {
  field: string
  isAnnotation: boolean
}

export type GroupJobsResponse = {
  count: number
  groups: JobGroup[]
}

export class GroupJobsService implements IGroupJobsService {
  constructor(private apiBase: string) {}

  async groupJobs(
    filters: JobFilter[],
    order: JobOrder,
    groupedField: GroupedField,
    aggregates: string[],
    skip: number,
    take: number,
    abortSignal: AbortSignal | undefined,
  ): Promise<GroupJobsResponse> {
    const response = await fetch(this.apiBase + "/api/v1/jobGroups", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        filters,
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
      count: json.count ?? 0,
      groups: json.groups ?? [],
    }
  }
}
