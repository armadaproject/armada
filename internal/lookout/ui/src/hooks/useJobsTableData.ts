import { useEffect, useState } from "react"

import {
  ColumnFiltersState,
  ColumnSort,
  ExpandedStateList,
  PaginationState,
  RowSelectionState,
  SortingState,
} from "@tanstack/react-table"
import { JobGroupRow, JobRow, JobTableRow } from "models/jobsTableModels"
import { Job, JobId, JobOrder } from "models/lookoutV2Models"
import { VariantType } from "notistack"
import { IGetJobsService } from "services/lookoutV2/GetJobsService"
import { IGroupJobsService } from "services/lookoutV2/GroupJobsService"
import { getErrorMessage } from "utils"
import { ColumnId, JobTableColumn, StandardColumnId } from "utils/jobsTableColumns"
import {
  fetchJobGroups,
  fetchJobs,
  FetchRowRequest,
  getFiltersForRows,
  groupsToRows,
  jobsToRows,
  PendingData,
} from "utils/jobsTableUtils"
import { fromRowId, mergeSubRows } from "utils/reactTableUtils"

export interface UseFetchJobsTableDataArgs {
  groupedColumns: ColumnId[]
  visibleColumns: ColumnId[]
  expandedState: ExpandedStateList
  sortingState: SortingState
  paginationState: PaginationState
  columnFilters: ColumnFiltersState
  allColumns: JobTableColumn[]
  selectedRows: RowSelectionState
  updateSelectedRows: (newState: RowSelectionState) => void
  getJobsService: IGetJobsService
  groupJobsService: IGroupJobsService
  openSnackbar: (message: string, variant: VariantType) => void
}

export interface UseFetchJobsTableDataResult {
  data: JobTableRow[]
  jobInfoMap: Map<JobId, Job>
  pageCount: number
  rowsToFetch: PendingData[]
  setRowsToFetch: (toFetch: PendingData[]) => void
  totalRowCount: number
}

const aggregatableFields = new Map<ColumnId, string>([
  [StandardColumnId.TimeSubmittedUtc, "submitted"],
  [StandardColumnId.TimeSubmittedAgo, "submitted"],
  [StandardColumnId.LastTransitionTimeUtc, "lastTransitionTime"],
  [StandardColumnId.TimeInState, "lastTransitionTime"],
])

export function columnIsAggregatable(columnId: ColumnId): boolean {
  return aggregatableFields.has(columnId)
}

const columnToJobSortFieldMap = new Map<ColumnId, string>([
  [StandardColumnId.JobID, "jobId"],
  [StandardColumnId.TimeSubmittedUtc, "submitted"],
  [StandardColumnId.TimeSubmittedAgo, "submitted"],
  [StandardColumnId.LastTransitionTimeUtc, "lastTransitionTime"],
  [StandardColumnId.TimeInState, "lastTransitionTime"],
])

const columnToGroupSortFieldMap = new Map<ColumnId, string>([
  [StandardColumnId.Count, "count"],
  [StandardColumnId.TimeSubmittedUtc, "submitted"],
  [StandardColumnId.TimeSubmittedAgo, "submitted"],
  [StandardColumnId.LastTransitionTimeUtc, "lastTransitionTime"],
  [StandardColumnId.TimeInState, "lastTransitionTime"],
])

// Return ordering to request to API based on column
function getOrder(sortedField: ColumnSort, isJobFetch: boolean): JobOrder {
  const defaultJobOrder: JobOrder = {
    field: "jobId",
    direction: "DESC",
  }

  const defaultGroupOrder: JobOrder = {
    field: "count",
    direction: "DESC",
  }

  let field = ""
  if (isJobFetch) {
    if (!columnToJobSortFieldMap.has(sortedField.id as ColumnId)) {
      return defaultJobOrder
    }
    field = columnToJobSortFieldMap.get(sortedField.id as ColumnId) as string
  } else {
    if (!columnToGroupSortFieldMap.has(sortedField.id as ColumnId)) {
      return defaultGroupOrder
    }
    field = columnToGroupSortFieldMap.get(sortedField.id as ColumnId) as string
  }

  return {
    field: field,
    direction: sortedField.desc ? "DESC" : "ASC",
  }
}

export const useFetchJobsTableData = ({
  groupedColumns,
  visibleColumns,
  expandedState,
  sortingState,
  paginationState,
  columnFilters,
  allColumns,
  selectedRows,
  updateSelectedRows,
  getJobsService,
  groupJobsService,
  openSnackbar,
}: UseFetchJobsTableDataArgs): UseFetchJobsTableDataResult => {
  const [data, setData] = useState<JobTableRow[]>([])
  const [jobInfoMap, setJobInfoMap] = useState<Map<JobId, Job>>(new Map())
  const [pendingData, setPendingData] = useState<PendingData[]>([])
  const [totalRowCount, setTotalRowCount] = useState(0)
  const [pageCount, setPageCount] = useState<number>(-1)

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

      const sortedField = sortingState[0]

      const order = getOrder(sortedField, isJobFetch)
      const rowRequest: FetchRowRequest = {
        filters: getFiltersForRows(columnFilters, allColumns, parentRowInfo?.rowIdPartsPath ?? []),
        skip: nextRequest.skip ?? 0,
        take: nextRequest.take ?? paginationState.pageSize,
        order: order,
      }

      let newData, totalCount
      try {
        if (isJobFetch) {
          const { jobs, count: totalJobs } = await fetchJobs(rowRequest, getJobsService, abortController.signal)
          newData = jobsToRows(jobs)
          totalCount = totalJobs

          setJobInfoMap(new Map([...jobInfoMap.entries(), ...jobs.map((j): [JobId, Job] => [j.jobId, j])]))
        } else {
          const groupedCol = groupedColumns[expandedLevel]

          const colsToAggregate = visibleColumns
            .filter((col) => aggregatableFields.has(col))
            .map((col) => aggregatableFields.get(col))
            .filter((val) => val !== undefined) as string[]
          const { groups, count: totalGroups } = await fetchJobGroups(
            rowRequest,
            groupJobsService,
            groupedCol,
            colsToAggregate,
            abortController.signal,
          )
          newData = groupsToRows(groups, parentRowInfo?.rowId, groupedCol)
          totalCount = totalGroups
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
        parentRow.subRowCount = totalCount

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
      if (parentRowInfo === undefined) {
        setPageCount(Math.ceil(totalCount / paginationState.pageSize))
        setTotalRowCount(totalCount)
      }
    }

    fetchData().catch(console.error)

    // This will run when the current invocation is no longer needed (either because the
    // component is unmounted, or the effect needs to run again)
    return () => {
      abortController.abort("Request is no longer needed")
    }
  }, [pendingData, paginationState, groupedColumns, expandedState, columnFilters, sortingState, allColumns])

  return {
    data,
    jobInfoMap,
    pageCount,
    rowsToFetch: pendingData,
    setRowsToFetch: setPendingData,
    totalRowCount,
  }
}
