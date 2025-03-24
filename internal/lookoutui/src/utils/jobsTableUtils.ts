import { ExpandedStateList, RowSelectionState, Updater } from "@tanstack/react-table"
import _ from "lodash"

import {
  ANNOTATION_COLUMN_PREFIX,
  AnnotationColumnId,
  DEFAULT_COLUMN_MATCHES,
  fromAnnotationColId,
  isStandardColId,
  VALID_COLUMN_MATCHES,
} from "./jobsTableColumns"
import { findRowInData, fromRowId, RowId, RowIdParts, toRowId } from "./reactTableUtils"
import { LookoutColumnFilter } from "../containers/lookout/JobsTableContainer"
import { isJobGroupRow, JobGroupRow, JobRow, JobTableRow } from "../models/jobsTableModels"
import { Job, JobFilter, JobFiltersWithExcludes, JobGroup, JobOrder, Match } from "../models/lookoutModels"
import { IGetJobsService } from "../services/lookout/GetJobsService"
import { GroupedField, IGroupJobsService } from "../services/lookout/GroupJobsService"

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
  let match: Match = Match.Exact // base case
  if (columnId in DEFAULT_COLUMN_MATCHES) {
    match = DEFAULT_COLUMN_MATCHES[columnId]
  }

  if (!(columnId in columnMatches)) {
    return match
  }

  const validMatches = isStandardColId(columnId)
    ? VALID_COLUMN_MATCHES[columnId]
    : VALID_COLUMN_MATCHES[ANNOTATION_COLUMN_PREFIX]

  if (!validMatches) {
    console.error(`There are no valid column matches for column with ID '${columnId}'`)
    return match
  }

  return validMatches.includes(columnMatches[columnId]) ? columnMatches[columnId] : validMatches[0]
}

// Returns a list of job filters, each with another list of job filters to exclude, to get the correct list of jobs
// which the user has selected. This recursive function is required since we allow grouping and for the user to select
// and de-select groups.
//
// Each row of the table is effectively a node in a tree, where job rows are leaf nodes, and group rows are internal
// nodes whose children are its sub-rows.
//
// For each node, starting at the root:
// - if it is a leaf node (i.e. a single job), we add it to the list of filters if it is selected
// - if it is an internal node, we add it to the list of filters if it is selected, but excluding all its children
// - we then recurse on all its children
export const getFiltersForRowsSelection = (
  rows: JobTableRow[],
  selectedRows: RowSelectionState,
  columnFilters: LookoutColumnFilter[],
  columnMatches: Record<string, Match>,
): JobFiltersWithExcludes[] =>
  rows.reduce<JobFiltersWithExcludes[]>((acc, row) => {
    const isRowSelected = selectedRows[row.rowId]
    const filtersForRow = getFiltersForRow(columnFilters, columnMatches, fromRowId(row.rowId).rowIdPartsPath)

    if (!isJobGroupRow(row) || row.subRows.length === 0) {
      if (isRowSelected) {
        acc.push({ jobFilters: filtersForRow, excludesJobFilters: [] })
      }
      return acc
    }

    if (isRowSelected) {
      acc.push({
        jobFilters: filtersForRow,
        excludesJobFilters: row.subRows.map((subRow) =>
          getFiltersForRow(columnFilters, columnMatches, fromRowId(subRow.rowId).rowIdPartsPath),
        ),
      })
    }

    acc.push(...getFiltersForRowsSelection(row.subRows, selectedRows, columnFilters, columnMatches))
    return acc
  }, [])

export function getFiltersForRow(
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
