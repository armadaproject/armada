import { ExpandedStateList, Updater } from "@tanstack/react-table"
import _ from "lodash"
import { JobGroupRow, JobRow, JobTableRow } from "models/jobsTableModels"
import { Job, JobFilter, JobGroup, JobOrder, Match } from "models/lookoutV2Models"
import { IGetJobsService } from "services/lookoutV2/GetJobsService"
import { IGroupJobsService } from "services/lookoutV2/GroupJobsService"

import { LookoutColumnFilter } from "../containers/lookoutV2/JobsTableContainer"
import { AnnotationColumnId, ColumnId, fromAnnotationColId, isStandardColId } from "./jobsTableColumns"
import { findRowInData, RowId, RowIdParts, toRowId } from "./reactTableUtils"

export interface PendingData {
  parentRowId: RowId | "ROOT"
  skip: number
  take?: number
  append?: boolean
}

export const pendingDataForAllVisibleData = (
  expanded: ExpandedStateList,
  data: JobTableRow[],
  defaultPageSize: number,
  rootSkip = 0,
): PendingData[] => {
  const expandedGroups: PendingData[] = Object.keys(expanded).map((rowId) => {
    const parentRow = findRowInData(data, rowId as RowId)
    const numSubRows = parentRow?.subRows.length ?? 0
    return {
      parentRowId: rowId as RowId,
      // Retain the same number of rows that are currently shown (unless it's smaller than the page size)
      // Since these are currently all retreived in one request, they could be slower
      // if there is a lot of expanded rows
      take: numSubRows > defaultPageSize ? numSubRows : defaultPageSize,
      skip: 0,
    }
  })

  const rootData: PendingData = { parentRowId: "ROOT" as PendingData["parentRowId"], skip: rootSkip }

  // Fetch the root data first, then any expanded subgroups
  return [rootData].concat(expandedGroups)
}

export function getFiltersForRows(filters: LookoutColumnFilter[], expandedRowIdParts: RowIdParts[]): JobFilter[] {
  const filterColumnsIndexes = new Map<string, number>()
  const jobFilters = filters.map(({ id, value, match }, i) => {
    const isArray = _.isArray(value)
    const isAnnotation = !isStandardColId(id)
    let field = id
    if (isAnnotation) {
      field = fromAnnotationColId(id as AnnotationColumnId)
    }

    filterColumnsIndexes.set(field, i)

    return {
      isAnnotation: isAnnotation,
      field: field,
      value: isArray ? (value as string[]) : (value as string),
      match: match,
    }
  })

  // Overwrite for expanded groups
  for (const rowIdParts of expandedRowIdParts) {
    const filter = {
      field: rowIdParts.type,
      value: rowIdParts.value,
      match: Match.Exact,
      isAnnotation: false,
    }
    if (filterColumnsIndexes.has(rowIdParts.type)) {
      const i = filterColumnsIndexes.get(rowIdParts.type) as number
      jobFilters[i] = filter
    } else {
      jobFilters.push(filter)
    }
  }

  return jobFilters
}

export interface FetchRowRequest {
  filters: JobFilter[]
  skip: number
  take: number
  order: JobOrder
}
export const fetchJobs = async (
  rowRequest: FetchRowRequest,
  getJobsService: IGetJobsService,
  abortSignal: AbortSignal,
) => {
  const { filters, skip, take, order } = rowRequest

  return await getJobsService.getJobs(filters, order, skip, take, abortSignal)
}

export const fetchJobGroups = async (
  rowRequest: FetchRowRequest,
  groupJobsService: IGroupJobsService,
  groupedColumn: string,
  columnsToAggregate: string[],
  abortSignal: AbortSignal,
) => {
  const { filters, skip, take, order } = rowRequest

  return await groupJobsService.groupJobs(filters, order, groupedColumn, columnsToAggregate, skip, take, abortSignal)
}

export const jobsToRows = (jobs: Job[]): JobRow[] => {
  return jobs.map(
    (job): JobRow => ({
      rowId: toRowId({ type: "jobId", value: job.jobId }),
      ...job,
    }),
  )
}

export const groupsToRows = (
  groups: JobGroup[],
  baseRowId: RowId | undefined,
  groupingField: ColumnId,
): JobGroupRow[] => {
  return groups.map((group): JobGroupRow => {
    const row: JobGroupRow = {
      rowId: toRowId({ type: groupingField, value: group.name, parentRowId: baseRowId }),
      [groupingField]: group.name,
      groupedField: groupingField,

      isGroup: true,
      jobCount: group.count,

      // Will be set later if expanded
      subRowCount: undefined,
      subRows: [],
    }
    for (const [key, val] of Object.entries(group.aggregates)) {
      switch (key) {
        case "submitted":
          row.submitted = val as string
          break
        case "lastTransitionTime":
          row.lastTransitionTime = val as string
          break
        default:
          break
      }
    }
    return row
  })
}

export const diffOfKeys = <K extends string | number | symbol>(
  currentObject?: Record<K, unknown>,
  oldObject?: Record<K, unknown>,
): [K[], K[]] => {
  const currentKeys = new Set(Object.keys(currentObject ?? {}) as K[])
  const prevKeys = new Set(Object.keys(oldObject ?? {}) as K[])

  const addedKeys = Array.from(currentKeys).filter((e) => !prevKeys.has(e))
  const removedKeys = Array.from(prevKeys).filter((e) => !currentKeys.has(e))
  return [addedKeys, removedKeys]
}

export const updaterToValue = <T>(updaterOrValue: Updater<T>, previousValue: T): T => {
  return typeof updaterOrValue === "function" ? (updaterOrValue as (old: T) => T)(previousValue) : updaterOrValue
}
