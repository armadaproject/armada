import React, { useCallback, useEffect, useMemo, useState } from "react"

import {
  TableContainer,
  Paper,
  Table,
  TableHead,
  TableRow,
  TableCell,
  TableBody,
  CircularProgress,
  TablePagination,
  TableFooter,
  Button,
  Box,
} from "@mui/material"
import {
  ColumnDef,
  ExpandedStateList,
  getCoreRowModel,
  getExpandedRowModel,
  getGroupedRowModel,
  getPaginationRowModel,
  PaginationState,
  Row,
  RowSelectionState,
  useReactTable,
  Updater,
  ExpandedState,
  ColumnFiltersState,
  SortingState,
  VisibilityState,
} from "@tanstack/react-table"
import { JobsTableActionBar } from "components/lookoutV2/JobsTableActionBar"
import { HeaderCell } from "components/lookoutV2/JobsTableCell"
import { JobsTableRow } from "components/lookoutV2/JobsTableRow"
import { Sidebar } from "components/lookoutV2/sidebar/Sidebar"
import _ from "lodash"
import { JobTableRow, isJobGroupRow, JobRow, JobGroupRow } from "models/jobsTableModels"
import { Job, JobFilter, JobId } from "models/lookoutV2Models"
import { useSnackbar } from "notistack"
import { IGetJobsService } from "services/lookoutV2/GetJobsService"
import { IGroupJobsService } from "services/lookoutV2/GroupJobsService"
import { UpdateJobsService } from "services/lookoutV2/UpdateJobsService"
import { getErrorMessage } from "utils"
import {
  ColumnId,
  DEFAULT_COLUMN_VISIBILITY,
  DEFAULT_GROUPING,
  JobTableColumn,
  JOB_COLUMNS,
  StandardColumnId,
  toColId,
} from "utils/jobsTableColumns"
import {
  convertRowPartsToFilters,
  fetchJobGroups,
  fetchJobs,
  groupsToRows,
  jobsToRows,
  diffOfKeys,
  updaterToValue,
  convertColumnFiltersToFilters,
  FetchRowRequest,
  PendingData,
  pendingDataForAllVisibleData,
} from "utils/jobsTableUtils"
import { fromRowId, mergeSubRows, RowId } from "utils/reactTableUtils"

import styles from "./JobsTableContainer.module.css"
import { useFetchJobsTableData } from "hooks/useJobsTableData"

const DEFAULT_PAGE_SIZE = 30

interface JobsTableContainerProps {
  getJobsService: IGetJobsService
  groupJobsService: IGroupJobsService
  updateJobsService: UpdateJobsService
  debug: boolean
}
export const JobsTableContainer = ({
  getJobsService,
  groupJobsService,
  updateJobsService,
  debug,
}: JobsTableContainerProps) => {
  const { enqueueSnackbar } = useSnackbar()

  // Columns
  const [allColumns, setAllColumns] = useState<JobTableColumn[]>(JOB_COLUMNS)
  const [columnVisibility, setColumnVisibility] = useState<VisibilityState>(DEFAULT_COLUMN_VISIBILITY)
  const visibleColumnIds = useMemo(
    () =>
      Object.keys(columnVisibility)
        .map(toColId)
        .filter((colId) => columnVisibility[colId]),
    [columnVisibility],
  )

  // Grouping
  const [grouping, setGrouping] = useState<ColumnId[]>(DEFAULT_GROUPING)

  // Expanding
  const [expanded, setExpanded] = useState<ExpandedStateList>({})

  // Selecting
  const [selectedRows, setSelectedRows] = useState<RowSelectionState>({})
  const [sidebarJob, setSidebarJob] = useState<Job | undefined>(undefined)

  // Pagination
  const [pagination, setPagination] = useState<PaginationState>({
    pageIndex: 0,
    pageSize: DEFAULT_PAGE_SIZE,
  })
  const { pageIndex, pageSize } = useMemo(() => pagination, [pagination])

  // Filtering
  const [columnFilterState, setColumnFilterState] = useState<ColumnFiltersState>([])

  // Sorting
  const [sorting, setSorting] = useState<SortingState>([{ id: "jobId", desc: true }])

  const {data, pageCount, rowsToFetch, setRowsToFetch, totalRowCount} = useFetchJobsTableData({
    groupedColumns: grouping, 
    expandedState: expanded,
    paginationState: pagination, 
    sortingState: sorting, 
    columnFilters: columnFilterState, 
    allColumns, 
    selectedRows, 
    updateSelectedRows: setSelectedRows, 
    getJobsService, 
    groupJobsService, 
    enqueueSnackbar, 
  })

  const onRefresh = useCallback(() => {
    setSelectedRows({})
    setRowsToFetch(pendingDataForAllVisibleData(expanded, data, pageSize, pageIndex * pageSize))
  }, [expanded, data, pageSize, pageIndex])

  const onColumnVisibilityChange = useCallback(
    (colIdToToggle: ColumnId) => {
      setColumnVisibility({
        ...columnVisibility,
        [colIdToToggle]: !columnVisibility[colIdToToggle],
      })
    },
    [columnVisibility],
  )

  const onGroupingChange = useCallback(
    (newState: ColumnId[]) => {
      // Reset currently expanded/selected when grouping changes
      setSelectedRows({})
      setSidebarJob(undefined)
      setExpanded({})

      // Check all grouping columns are displayed
      setColumnVisibility(newState.reduce((a, s) => ({ ...a, [s]: true }), columnVisibility))

      setGrouping([...newState])

      // Refetch the root data
      setRowsToFetch([{ parentRowId: "ROOT", skip: 0 }])
    },
    [allColumns, columnVisibility],
  )

  const onRootPaginationChange = useCallback(
    (updater: Updater<PaginationState>) => {
      const newPagination = updaterToValue(updater, pagination)
      // Reset currently expanded/selected when grouping changes
      // TODO: Consider allowing rows to be selected across pages?
      setSelectedRows({})
      setSidebarJob(undefined)
      setExpanded({})
      setPagination(newPagination)

      // Refetch the root data
      setRowsToFetch([{ parentRowId: "ROOT", skip: newPagination.pageIndex * pageSize }])
    },
    [pagination, pageSize],
  )

  const onLoadMoreSubRows = useCallback((rowId: RowId, skip: number) => {
    setRowsToFetch([{ parentRowId: rowId, skip, append: true }])
  }, [])

  const onExpandedChange = useCallback(
    (updater: Updater<ExpandedState>) => {
      const newExpandedOrBool = updaterToValue(updater, expanded)
      const newExpandedState =
        typeof newExpandedOrBool === "boolean"
          ? _.fromPairs(table.getRowModel().flatRows.map((r) => [r.id, true]))
          : newExpandedOrBool
      const [newlyExpanded] = diffOfKeys<RowId>(newExpandedState, expanded)
      setExpanded(newExpandedState)

      // Fetch subrows for expanded rows
      setRowsToFetch(newlyExpanded.map((rowId) => ({ parentRowId: rowId, skip: 0 })))
    },
    [expanded],
  )

  const onSelectedRowChange = useCallback(
    (updater: Updater<RowSelectionState>) => {
      const newSelectedRows = updaterToValue(updater, selectedRows)
      setSelectedRows(newSelectedRows)
    },
    [selectedRows],
  )

  const onFilterChange = useCallback(
    (updater: Updater<ColumnFiltersState>) => {
      const newFilterState = updaterToValue(updater, columnFilterState)
      setColumnFilterState(newFilterState)
      setSelectedRows({})
      setSidebarJob(undefined)
      setRowsToFetch(pendingDataForAllVisibleData(expanded, data, pageSize))
    },
    [columnFilterState, expanded, data, pageSize],
  )

  const onSortingChange = useCallback(
    (updater: Updater<SortingState>) => {
      const newSortingState = updaterToValue(updater, sorting)
      setSorting(newSortingState)

      // Refetch any expanded subgroups, and root data with updated sorting params
      setRowsToFetch(pendingDataForAllVisibleData(expanded, data, pageSize, pageIndex * pageSize))
    },
    [sorting, expanded, pageIndex, pageSize, data],
  )

  const onJobRowClick = useCallback(
    (jobRow: JobRow) => {
      const clickedJob = jobRow as Job
      setSidebarJob(sidebarJob?.jobId !== clickedJob.jobId ? clickedJob : undefined)
    },
    [sidebarJob],
  )
  const onSideBarClose = useCallback(() => setSidebarJob(undefined), [])

  const selectedItemsFilters: JobFilter[][] = useMemo(() => {
    const tableFilters = convertColumnFiltersToFilters(columnFilterState, allColumns)
    return Object.keys(selectedRows).map((rowId) => {
      const { rowIdPartsPath } = fromRowId(rowId as RowId)
      return tableFilters.concat(convertRowPartsToFilters(rowIdPartsPath))
    })
  }, [selectedRows, columnFilterState, allColumns])

  const table = useReactTable({
    data: data ?? [],
    columns: allColumns,
    state: {
      grouping,
      expanded,
      pagination,
      columnFilters: columnFilterState,
      rowSelection: selectedRows,
      columnPinning: {
        left: [StandardColumnId.SelectorCol],
      },
      columnVisibility,
      sorting,
    },
    getCoreRowModel: getCoreRowModel(),
    getRowId: (row) => row.rowId,
    getSubRows: (row) => (isJobGroupRow(row) && row.subRows) || undefined,

    // Selection
    enableRowSelection: true,
    enableMultiRowSelection: true,
    enableSubRowSelection: true,
    onRowSelectionChange: onSelectedRowChange,

    // Grouping
    manualGrouping: true,
    getGroupedRowModel: getGroupedRowModel(),
    getExpandedRowModel: getExpandedRowModel(),
    onExpandedChange: onExpandedChange,
    autoResetExpanded: false,
    manualExpanding: false,

    // Pagination
    manualPagination: true,
    pageCount: pageCount,
    paginateExpandedRows: true,
    onPaginationChange: onRootPaginationChange,
    getPaginationRowModel: getPaginationRowModel(),

    // Filtering
    onColumnFiltersChange: onFilterChange,
    manualFiltering: true,

    // Sorting
    onSortingChange: onSortingChange,
  })

  const topLevelRows = table.getRowModel().rows.filter((row) => row.depth === 0)
  return (
    <Box sx={{ display: "flex" }}>
      <Box sx={{ overflowX: "auto", overflowY: "auto", margin: "0.5em" }}>
        <JobsTableActionBar
          isLoading={rowsToFetch.length > 0}
          allColumns={allColumns}
          groupedColumns={grouping}
          visibleColumns={visibleColumnIds}
          selectedItemFilters={selectedItemsFilters}
          onRefresh={onRefresh}
          onColumnsChanged={setAllColumns}
          onGroupsChanged={onGroupingChange}
          toggleColumnVisibility={onColumnVisibilityChange}
          getJobsService={getJobsService}
          updateJobsService={updateJobsService}
        />
        <TableContainer component={Paper}>
          <Table sx={{ tableLayout: "fixed" }}>
            <TableHead>
              {table.getHeaderGroups().map((headerGroup) => (
                <TableRow key={headerGroup.id}>
                  {headerGroup.headers.map((header) => (
                    <HeaderCell header={header} key={header.id} />
                  ))}
                </TableRow>
              ))}
            </TableHead>

            <JobsTableBody
              dataIsLoading={rowsToFetch.length > 0}
              columns={table.getVisibleLeafColumns()}
              topLevelRows={topLevelRows}
              sidebarJobId={sidebarJob?.jobId}
              onLoadMoreSubRows={onLoadMoreSubRows}
              onClickJobRow={onJobRowClick}
            />

            <TableFooter>
              <TableRow>
                <TablePagination
                  rowsPerPageOptions={[3, 10, 20, 30, 40, 50]}
                  count={totalRowCount}
                  rowsPerPage={pageSize}
                  page={pageIndex}
                  onPageChange={(_, page) => table.setPageIndex(page)}
                  onRowsPerPageChange={(e) => table.setPageSize(Number(e.target.value))}
                  colSpan={table.getVisibleLeafColumns().length}
                  showFirstButton={true}
                  showLastButton={true}
                />
              </TableRow>
            </TableFooter>
          </Table>
        </TableContainer>

        {debug && <pre>{JSON.stringify(table.getState(), null, 2)}</pre>}
      </Box>

      {sidebarJob !== undefined && <Sidebar job={sidebarJob} onClose={onSideBarClose} />}
    </Box>
  )
}

interface JobsTableBodyProps {
  dataIsLoading: boolean
  columns: ColumnDef<JobTableRow>[]
  topLevelRows: Row<JobTableRow>[]
  sidebarJobId: JobId | undefined
  onLoadMoreSubRows: (rowId: RowId, skip: number) => void
  onClickJobRow: (row: JobRow) => void
}
const JobsTableBody = ({
  dataIsLoading,
  columns,
  topLevelRows,
  sidebarJobId,
  onLoadMoreSubRows,
  onClickJobRow,
}: JobsTableBodyProps) => {
  const canDisplay = !dataIsLoading && topLevelRows.length > 0
  return (
    <TableBody>
      {!canDisplay && (
        <TableRow>
          {dataIsLoading && topLevelRows.length === 0 && (
            <TableCell colSpan={columns.length}>
              <CircularProgress />
            </TableCell>
          )}
          {!dataIsLoading && topLevelRows.length === 0 && (
            <TableCell colSpan={columns.length}>There is no data to display</TableCell>
          )}
        </TableRow>
      )}

      {topLevelRows.map((row) => recursiveRowRender(row, sidebarJobId, onLoadMoreSubRows, onClickJobRow))}
    </TableBody>
  )
}

const recursiveRowRender = (
  row: Row<JobTableRow>,
  sidebarJobId: JobId | undefined,
  onLoadMoreSubRows: (rowId: RowId, skip: number) => void,
  onClickJobRow: (row: JobRow) => void,
): JSX.Element => {
  const original = row.original
  const rowIsGroup = isJobGroupRow(original)

  const depthGaugeLevelThicknessPixels = 6
  const isOpenInSidebar = sidebarJobId !== undefined && original.jobId === sidebarJobId

  return (
    <React.Fragment key={`${row.id}_d${row.depth}`}>
      {/* Render the current row */}
      <JobsTableRow row={row} isOpenInSidebar={isOpenInSidebar} onClick={!rowIsGroup ? onClickJobRow : undefined} />

      {/* Render any sub rows if expanded */}
      {rowIsGroup &&
        row.getIsExpanded() &&
        row.subRows.map((row) => recursiveRowRender(row, sidebarJobId, onLoadMoreSubRows, onClickJobRow))}

      {/* Render pagination tools for this expanded row */}
      {rowIsGroup && row.getIsExpanded() && (original.subRowCount ?? 0) > original.subRows.length && (
        <TableRow
          className={[styles.rowDepthIndicator, styles.loadMoreRow].join(" ")}
          sx={{ backgroundSize: (row.depth + 1) * depthGaugeLevelThicknessPixels }}
        >
          <TableCell colSpan={row.getVisibleCells().length} align="center" size="small">
            <Button
              size="small"
              variant="text"
              onClick={() => onLoadMoreSubRows(row.id as RowId, original.subRows.length)}
            >
              Load more
            </Button>
          </TableCell>
        </TableRow>
      )}
    </React.Fragment>
  )
}
