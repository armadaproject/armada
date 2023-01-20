import { useEffect, useState } from "react"

import {
  SortingState,
  ColumnFiltersState,
  RowSelectionState,
  PaginationState,
  ExpandedStateList,
} from "@tanstack/react-table"
import { JobTableRow, JobRow, JobGroupRow } from "models/jobsTableModels"
import { Job, JobId } from "models/lookoutV2Models"
import { SnackbarProvider } from "notistack"
import { IGetJobsService } from "services/lookoutV2/GetJobsService"
import { IGroupJobsService } from "services/lookoutV2/GroupJobsService"
import { getErrorMessage } from "utils"
import { ColumnId, JobTableColumn } from "utils/jobsTableColumns"
import {
  PendingData,
  FetchRowRequest,
  convertRowPartsToFilters,
  convertColumnFiltersToFilters,
  fetchJobs,
  jobsToRows,
  fetchJobGroups,
  groupsToRows,
} from "utils/jobsTableUtils"
import { fromRowId, mergeSubRows } from "utils/reactTableUtils"

export interface UseFetchJobsTableDataArgs {
  groupedColumns: ColumnId[]
  expandedState: ExpandedStateList
  sortingState: SortingState
  paginationState: PaginationState
  columnFilters: ColumnFiltersState
  allColumns: JobTableColumn[]
  selectedRows: RowSelectionState
  updateSelectedRows: (newState: RowSelectionState) => void
  getJobsService: IGetJobsService
  groupJobsService: IGroupJobsService
  enqueueSnackbar: SnackbarProvider["enqueueSnackbar"]
}
export interface UseFetchJobsTableDataResult {
  data: JobTableRow[]
  jobInfoMap: Map<JobId, Job>
  pageCount: number
  rowsToFetch: PendingData[]
  setRowsToFetch: (toFetch: PendingData[]) => void
  totalRowCount: number
}
export const useFetchJobsTableData = ({
  groupedColumns,
  expandedState,
  sortingState,
  paginationState,
  columnFilters,
  allColumns,
  selectedRows,
  updateSelectedRows,
  getJobsService,
  groupJobsService,
  enqueueSnackbar,
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

      const rowRequest: FetchRowRequest = {
        filters: [
          ...convertRowPartsToFilters(parentRowInfo?.rowIdPartsPath ?? []),
          ...convertColumnFiltersToFilters(columnFilters, allColumns),
        ],
        skip: nextRequest.skip ?? 0,
        take: nextRequest.take ?? paginationState.pageSize,
        order: { field: sortedField.id, direction: sortedField.desc ? "DESC" : "ASC" },
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

          // TODO: Wire in aggregatable+visible columns (maybe use column metadata?)
          const colsToAggregate: string[] = []
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
        enqueueSnackbar("Failed to retrieve jobs. Error: " + errMsg, { variant: "error" })
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
