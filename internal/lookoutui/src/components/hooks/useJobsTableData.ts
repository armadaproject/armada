import { useEffect, useState } from "react"

import { ExpandedStateList, PaginationState, RowSelectionState } from "@tanstack/react-table"
import { VariantType } from "notistack"

import { LookoutColumnOrder } from "../../common/jobsTableColumns"
import {
  AnnotationColumnId,
  ColumnId,
  fromAnnotationColId,
  isStandardColId,
  JobTableColumn,
  StandardColumnId,
} from "../../common/jobsTableColumns"
import { LookoutColumnFilter } from "../../common/jobsTableUtils"
import {
  getFiltersForGroupedAnnotations,
  getFiltersForRow,
  groupsToRows,
  jobsToRows,
  PendingData,
} from "../../common/jobsTableUtils"
import { fromRowId, mergeSubRows } from "../../common/reactTableUtils"
import { getErrorMessage } from "../../common/utils"
import { getConfig } from "../../config"
import { JobGroupRow, JobRow, JobTableRow } from "../../models/jobsTableModels"
import { AggregateType, Job, JobFilter, JobId, JobOrder, Match } from "../../models/lookoutModels"
import { useAuthenticatedFetch } from "../../oidcAuth"
import { GroupedField, IGroupJobsService } from "../../services/lookout/GroupJobsService"
import { GetJobsResponse } from "../../services/lookout/useGetJobs"

export interface UseFetchJobsTableDataArgs {
  groupedColumns: ColumnId[]
  visibleColumns: ColumnId[]
  expandedState: ExpandedStateList
  lookoutOrder: LookoutColumnOrder
  lookoutFilters: LookoutColumnFilter[]
  activeJobSets: boolean
  columnMatches: Record<string, Match>
  paginationState: PaginationState
  allColumns: JobTableColumn[]
  selectedRows: RowSelectionState
  updateSelectedRows: (newState: RowSelectionState) => void
  groupJobsService: IGroupJobsService
  openSnackbar: (message: string, variant: VariantType) => void
  lastTransitionTimeAggregate?: AggregateType
}

export interface UseFetchJobsTableDataResult {
  data: JobTableRow[]
  jobInfoMap: Map<JobId, Job>
  rowsToFetch: PendingData[]
  setRowsToFetch: (toFetch: PendingData[]) => void
}

const aggregatableFields = new Map<ColumnId, string>([
  [StandardColumnId.TimeSubmittedUtc, "submitted"],
  [StandardColumnId.TimeSubmittedAgo, "submitted"],
  [StandardColumnId.LastTransitionTimeUtc, "lastTransitionTime"],
  [StandardColumnId.TimeInState, "lastTransitionTime"],
  [StandardColumnId.State, "state"],
])

const groupableFields = new Map<ColumnId, string>([
  [StandardColumnId.Queue, "queue"],
  [StandardColumnId.Namespace, "namespace"],
  [StandardColumnId.JobSet, "jobSet"],
  [StandardColumnId.State, "state"],
])

export function columnIsAggregatable(columnId: ColumnId): boolean {
  return aggregatableFields.has(columnId)
}

const columnToJobSortFieldMap = new Map<ColumnId, string>([
  [StandardColumnId.JobID, "jobId"],
  [StandardColumnId.JobSet, "jobSet"],
  [StandardColumnId.TimeSubmittedUtc, "submitted"],
  [StandardColumnId.TimeSubmittedAgo, "submitted"],
  [StandardColumnId.LastTransitionTimeUtc, "lastTransitionTime"],
  [StandardColumnId.TimeInState, "lastTransitionTime"],
  [StandardColumnId.Queue, "queue"],
  [StandardColumnId.State, "state"],
])

const columnToGroupSortFieldMap = new Map<ColumnId, string>([
  [StandardColumnId.Count, "count"],
  [StandardColumnId.TimeSubmittedUtc, "submitted"],
  [StandardColumnId.TimeSubmittedAgo, "submitted"],
  [StandardColumnId.LastTransitionTimeUtc, "lastTransitionTime"],
  [StandardColumnId.TimeInState, "lastTransitionTime"],
  [StandardColumnId.JobSet, "jobSet"],
])

const defaultJobOrder: JobOrder = {
  field: "jobId",
  direction: "DESC",
}

const defaultGroupOrder: JobOrder = {
  field: "count",
  direction: "DESC",
}

// Return ordering to request to API based on column
function getOrder(lookoutOrder: LookoutColumnOrder, isJobFetch: boolean): JobOrder {
  let field = ""
  if (isJobFetch) {
    if (!columnToJobSortFieldMap.has(lookoutOrder.id as ColumnId)) {
      return defaultJobOrder
    }
    field = columnToJobSortFieldMap.get(lookoutOrder.id as ColumnId) as string
  } else {
    // If it is JobGroups, always return the order value here which might be overridden later
    if (!columnToGroupSortFieldMap.has(lookoutOrder.id as ColumnId)) {
      return defaultGroupOrder
    }
    field = columnToGroupSortFieldMap.get(lookoutOrder.id as ColumnId) as string
  }

  return {
    field: field,
    direction: lookoutOrder.direction,
  }
}

export const useFetchJobsTableData = ({
  groupedColumns,
  visibleColumns,
  expandedState,
  lookoutOrder,
  paginationState,
  lookoutFilters,
  activeJobSets,
  columnMatches,
  allColumns,
  selectedRows,
  updateSelectedRows,
  groupJobsService,
  openSnackbar,
  lastTransitionTimeAggregate,
}: UseFetchJobsTableDataArgs): UseFetchJobsTableDataResult => {
  const [data, setData] = useState<JobTableRow[]>([])
  const [jobInfoMap, setJobInfoMap] = useState<Map<JobId, Job>>(new Map())
  const [pendingData, setPendingData] = useState<PendingData[]>([])

  const authenticatedFetch = useAuthenticatedFetch()

  useEffect(() => {
    const abortController = new AbortController()

    async function fetchData() {
      if (pendingData.length === 0) {
        return
      }

      const [nextRequest, ...restOfRequests] = pendingData

      const parentRowInfo = nextRequest.parentRowId !== "ROOT" ? fromRowId(nextRequest.parentRowId) : undefined

      const groupingLevel = groupedColumns.length
      const expandedLevel = parentRowInfo ? parentRowInfo.rowIdPathFromRoot.length : 0
      const isJobFetch = expandedLevel === groupingLevel

      const filterValues = getFiltersForRow(lookoutFilters, columnMatches, parentRowInfo?.rowIdPartsPath ?? [])
      const order = getOrder(lookoutOrder, isJobFetch)
      const rowRequest: FetchRowRequest = {
        filters: filterValues,
        activeJobSets: activeJobSets,
        skip: nextRequest.skip ?? paginationState.pageIndex * paginationState.pageSize,
        take: nextRequest.take ?? paginationState.pageSize,
        order: order,
      }

      let newData
      try {
        if (isJobFetch) {
          const { jobs } = await fetchJobs(authenticatedFetch, rowRequest, abortController.signal)
          newData = jobsToRows(jobs)

          setJobInfoMap(new Map([...jobInfoMap.entries(), ...jobs.map((j): [JobId, Job] => [j.jobId, j])]))
        } else {
          const groupedCol = groupedColumns[expandedLevel]
          const groupedField = columnToGroupedField(groupedCol, lastTransitionTimeAggregate)

          // Override the group order if needed
          if (
            rowRequest.order.field !== groupedCol &&
            Array.from(groupableFields.values()).includes(rowRequest.order.field)
          ) {
            rowRequest.order = defaultGroupOrder
          }

          // Only relevant if we are grouping by annotations: Filter by all remaining annotations in the group by filter
          rowRequest.filters.push(...getFiltersForGroupedAnnotations(groupedColumns.slice(expandedLevel + 1)))

          const colsToAggregate = getColsToAggregate(visibleColumns, rowRequest.filters)
          const { groups } = await fetchJobGroups(
            authenticatedFetch,
            rowRequest,
            groupJobsService,
            groupedField,
            colsToAggregate,
            abortController.signal,
          )
          newData = groupsToRows(groups, parentRowInfo?.rowId, groupedField)
        }
      } catch (err) {
        if (abortController.signal.aborted) {
          return
        }

        const errMsg = await getErrorMessage(err)
        openSnackbar("Failed to retrieve jobs. Error: " + errMsg, "error")
        return
      }

      const { rootData, parentRow } = mergeSubRows<JobRow, JobGroupRow>(
        data,
        newData,
        parentRowInfo?.rowId,
        Boolean(nextRequest.append),
      )

      if (parentRow) {
        // Update any new children of selected rows
        if (parentRow.rowId in selectedRows) {
          const newSelectedRows = parentRow.subRows.reduce(
            (newSelectedSubRows, subRow) => {
              newSelectedSubRows[subRow.rowId] = true
              return newSelectedSubRows
            },
            { ...selectedRows },
          )
          updateSelectedRows(newSelectedRows)
        }
      }

      setData([...rootData]) // ReactTable will only re-render if the array identity changes
      setPendingData(restOfRequests)
    }

    // eslint-disable-next-line no-console
    fetchData().catch(console.error)

    // This will run when the current invocation is no longer needed (either because the
    // component is unmounted, or the effect needs to run again)
    return () => {
      abortController.abort("Request is no longer needed")
    }
  }, [
    pendingData,
    paginationState,
    groupedColumns,
    expandedState,
    lookoutFilters,
    columnMatches,
    lookoutOrder,
    allColumns,
    lastTransitionTimeAggregate,
  ])

  return {
    data,
    jobInfoMap,
    rowsToFetch: pendingData,
    setRowsToFetch: setPendingData,
  }
}

const columnToGroupedField = (colId: ColumnId, lastTransitionTimeAggregate?: AggregateType): GroupedField => {
  if (isStandardColId(colId)) {
    return {
      field: colId,
      isAnnotation: false,
      lastTransitionTimeAggregate,
    }
  }
  return {
    field: fromAnnotationColId(colId as AnnotationColumnId),
    isAnnotation: true,
    lastTransitionTimeAggregate,
  }
}

const getColsToAggregate = (visibleCols: ColumnId[], filters: JobFilter[]): string[] => {
  const aggregates = visibleCols
    .filter((col) => aggregatableFields.has(col))
    .map((col) => aggregatableFields.get(col))
    .filter((val) => val !== undefined) as string[]

  const stateIndex = aggregates.indexOf(StandardColumnId.State)
  if (stateIndex > -1) {
    const stateFilter = filters.find((f) => f.field === StandardColumnId.State)
    if (
      stateFilter &&
      ((stateFilter.match === Match.AnyOf && (stateFilter.value as string[]).length === 1) ||
        stateFilter.match === Match.Exact)
    ) {
      aggregates.splice(stateIndex, 1)
    }
  }
  return aggregates
}
export const fetchJobs = async (
  fetchFunc: GlobalFetch["fetch"],
  rowRequest: FetchRowRequest,
  abortSignal: AbortSignal,
): Promise<GetJobsResponse> => {
  const { filters, activeJobSets, skip, take, order } = rowRequest
  const config = getConfig()

  let path = "/api/v1/jobs"
  if (config.backend) {
    path += "?" + new URLSearchParams({ backend: config.backend })
  }

  const response = await fetchFunc(path, {
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

export interface FetchRowRequest {
  filters: JobFilter[]
  activeJobSets: boolean
  skip: number
  take: number
  order: JobOrder
}
export const fetchJobGroups = async (
  fetchFunc: GlobalFetch["fetch"],
  rowRequest: FetchRowRequest,
  groupJobsService: IGroupJobsService,
  groupedColumn: GroupedField,
  columnsToAggregate: string[],
  abortSignal: AbortSignal,
) => {
  const { filters, activeJobSets, skip, take, order } = rowRequest
  return await groupJobsService.groupJobs(
    fetchFunc,
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
