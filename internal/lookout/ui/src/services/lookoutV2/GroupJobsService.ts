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
    const response = await fetch("/api/v1/jobGroups", {
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
