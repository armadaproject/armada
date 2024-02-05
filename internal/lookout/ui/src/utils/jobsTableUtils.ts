import { ExpandedStateList, Updater } from "@tanstack/react-table"
import _ from "lodash"
import { JobGroupRow, JobRow, JobTableRow } from "models/jobsTableModels"
import { Job, JobFilter, JobGroup, JobOrder, Match } from "models/lookoutV2Models"
import { IGetJobsService } from "services/lookoutV2/GetJobsService"
import { GroupedField, IGroupJobsService } from "services/lookoutV2/GroupJobsService"

import { AnnotationColumnId, DEFAULT_COLUMN_MATCHES, fromAnnotationColId, isStandardColId } from "./jobsTableColumns"
import { findRowInData, RowId, RowIdParts, toRowId } from "./reactTableUtils"
import { LookoutColumnFilter } from "../containers/lookoutV2/JobsTableContainer"

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

export const matchForColumn = (columnId: string, columnMatches: Record<string, Match>) => {
  let match: Match = Match.StartsWith // base case if undefined (annotations)
  if (columnId in DEFAULT_COLUMN_MATCHES) {
    match = DEFAULT_COLUMN_MATCHES[columnId]
  }
  if (columnId in columnMatches) {
    match = columnMatches[columnId]
  }
  return match
}

export function getFiltersForRows(
  filters: LookoutColumnFilter[],
  columnMatches: Record<string, Match>,
  expandedRowIdParts: RowIdParts[],
): JobFilter[] {
  const filterColumnsIndexes = new Map<string, number>()
  const jobFilters = filters.map(({ id, value }, i) => {
    const isArray = _.isArray(value)
    const isAnnotation = !isStandardColId(id)
    let field = id
    if (isAnnotation) {
      field = fromAnnotationColId(id as AnnotationColumnId)
    }
    filterColumnsIndexes.set(field, i)
    const match = matchForColumn(id, columnMatches)
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
    if (!isStandardColId(rowIdParts.type)) {
      filter.isAnnotation = true
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

export function getFiltersForGroupedAnnotations(remainingGroups: string[]): JobFilter[] {
  return remainingGroups
    .filter((group) => !isStandardColId(group))
    .map((annotationColId) => {
      return {
        field: fromAnnotationColId(annotationColId as AnnotationColumnId),
        value: "",
        match: Match.Exists,
        isAnnotation: true,
      }
    })
}

export interface FetchRowRequest {
  filters: JobFilter[]
  activeJobSets: boolean
  skip: number
  take: number
  order: JobOrder
}
export const fetchJobs = async (
  rowRequest: FetchRowRequest,
  getJobsService: IGetJobsService,
  abortSignal: AbortSignal,
) => {
  const { filters, activeJobSets, skip, take, order } = rowRequest

  return await getJobsService.getJobs(filters, activeJobSets, order, skip, take, abortSignal)
}

export const fetchJobGroups = async (
  rowRequest: FetchRowRequest,
  groupJobsService: IGroupJobsService,
  groupedColumn: GroupedField,
  columnsToAggregate: string[],
  abortSignal: AbortSignal,
) => {
  const { filters, activeJobSets, skip, take, order } = rowRequest
  return await groupJobsService.groupJobs(
    filters,
    activeJobSets,
    order,
    groupedColumn,
    columnsToAggregate,
    skip,
    take,
    abortSignal,
  )
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
  groupedField: GroupedField,
): JobGroupRow[] => {
  return groups.map((group): JobGroupRow => {
    const row: JobGroupRow = {
      rowId: toRowId({ type: groupedField.field, value: group.name, parentRowId: baseRowId }),
      groupedField: groupedField.field,
      [groupedField.field]: group.name,
      stateCounts: undefined,

      isGroup: true,
      jobCount: group.count,

      // Will be set later if expanded
      subRowCount: undefined,
      subRows: [],
    }
    if (groupedField.isAnnotation) {
      row.annotations = {
        [groupedField.field]: group.name,
      }
    }
    for (const [key, val] of Object.entries(group.aggregates)) {
      switch (key) {
        case "submitted":
          row.submitted = val as string
          break
        case "lastTransitionTime":
          row.lastTransitionTime = val as string
          break
        case "state":
          row.stateCounts = val as Record<string, number>
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
