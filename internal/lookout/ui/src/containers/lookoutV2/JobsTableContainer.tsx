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
import { columnIsAggregatable, useFetchJobsTableData } from "hooks/useJobsTableData"
import _ from "lodash"
import { JobTableRow, isJobGroupRow, JobRow } from "models/jobsTableModels"
import { Job, JobFilter, JobId } from "models/lookoutV2Models"
import { IGetJobsService } from "services/lookoutV2/GetJobsService"
import { IGetRunErrorService } from "services/lookoutV2/GetRunErrorService"
import { IGroupJobsService } from "services/lookoutV2/GroupJobsService"
import { JobsTablePreferencesService } from "services/lookoutV2/JobsTablePreferencesService"
import { UpdateJobsService } from "services/lookoutV2/UpdateJobsService"
import { ColumnId, JobTableColumn, StandardColumnId, toColId } from "utils/jobsTableColumns"
import {
  convertRowPartsToFilters,
  diffOfKeys,
  updaterToValue,
  convertColumnFiltersToFilters,
  pendingDataForAllVisibleData,
  PendingData,
} from "utils/jobsTableUtils"
import { fromRowId, RowId } from "utils/reactTableUtils"

import { useCustomSnackbar } from "../../hooks/useCustomSnackbar"
import styles from "./JobsTableContainer.module.css"

const PAGE_SIZE_OPTIONS = [5, 25, 50, 100]

interface JobsTableContainerProps {
  jobsTablePreferencesService: JobsTablePreferencesService
  getJobsService: IGetJobsService
  groupJobsService: IGroupJobsService
  updateJobsService: UpdateJobsService
  runErrorService: IGetRunErrorService
  debug: boolean
}
export const JobsTableContainer = ({
  jobsTablePreferencesService,
  getJobsService,
  groupJobsService,
  updateJobsService,
  runErrorService,
  debug,
}: JobsTableContainerProps) => {
  const openSnackbar = useCustomSnackbar()

  const initialPrefs = useMemo(() => jobsTablePreferencesService.getInitialUserPrefs(), [])

  // Columns
  const [allColumns, setAllColumns] = useState<JobTableColumn[]>(initialPrefs.allColumnsInfo)
  const [columnVisibility, setColumnVisibility] = useState<VisibilityState>(initialPrefs.visibleColumns)
  const visibleColumnIds = useMemo(
    () =>
      Object.keys(columnVisibility)
        .map(toColId)
        .filter((colId) => columnVisibility[colId]),
    [columnVisibility],
  )

  // Grouping
  const [grouping, setGrouping] = useState<ColumnId[]>(initialPrefs.groupedColumns)

  // Expanding
  const [expanded, setExpanded] = useState<ExpandedStateList>(initialPrefs.expandedState)

  // Selecting
  const [selectedRows, setSelectedRows] = useState<RowSelectionState>({})
  const [sidebarJobId, setSidebarJobId] = useState<JobId | undefined>(initialPrefs.sidebarJobId)

  // Pagination
  const [pagination, setPagination] = useState<PaginationState>({
    pageIndex: initialPrefs.pageIndex,
    pageSize: initialPrefs.pageSize,
  })
  const { pageIndex, pageSize } = useMemo(() => pagination, [pagination])

  // Filtering
  const [columnFilterState, setColumnFilterState] = useState<ColumnFiltersState>(initialPrefs.filterState)

  // Sorting
  const [sorting, setSorting] = useState<SortingState>(initialPrefs.sortingState)

  // Data
  const { data, jobInfoMap, pageCount, rowsToFetch, setRowsToFetch, totalRowCount } = useFetchJobsTableData({
    groupedColumns: grouping,
    visibleColumns: visibleColumnIds,
    expandedState: expanded,
    paginationState: pagination,
    sortingState: sorting,
    columnFilters: columnFilterState,
    allColumns,
    selectedRows,
    updateSelectedRows: setSelectedRows,
    getJobsService,
    groupJobsService,
    openSnackbar,
  })

  // Check if there are grouped columns in initial configuration, and if so enable count column
  useEffect(() => {
    if (grouping.length > 0) {
      setColumnVisibility({
        ...columnVisibility,
        [StandardColumnId.Count]: true,
      })
    }
  }, [])

  // Retrieve data for any expanded rows from intial query param state
  useEffect(() => {
    const rowsToFetch: PendingData[] = [
      { parentRowId: "ROOT", skip: 0 },
      ...Object.keys(initialPrefs.expandedState).map((rowId) => ({ parentRowId: rowId as RowId, skip: 0 })),
    ]
    setRowsToFetch(rowsToFetch)
  }, [initialPrefs])

  // Find the job details for the selected job
  const sidebarJobDetails = useMemo(
    () => (sidebarJobId !== undefined ? jobInfoMap.get(sidebarJobId) : undefined),
    [sidebarJobId, jobInfoMap],
  )

  // Update query params with table state
  useEffect(() => {
    jobsTablePreferencesService.saveNewPrefs({
      groupedColumns: grouping,
      expandedState: expanded,
      pageIndex,
      pageSize,
      sortingState: sorting,
      filterState: columnFilterState,
      allColumnsInfo: allColumns,
      visibleColumns: columnVisibility,
      sidebarJobId: sidebarJobId,
    })
  }, [
    grouping,
    expanded,
    pageIndex,
    pageSize,
    sorting,
    columnFilterState,
    allColumns,
    columnVisibility,
    selectedRows,
    sidebarJobId,
  ])

  const onRefresh = useCallback(() => {
    setSelectedRows({})
    setRowsToFetch(pendingDataForAllVisibleData(expanded, data, pageSize, pageIndex * pageSize))
  }, [expanded, data, pageSize, pageIndex])

  const onColumnVisibilityChange = useCallback(
    (colIdToToggle: ColumnId) => {
      // Refresh if we make a new aggregate column visible
      let shouldRefresh = false
      if (columnIsAggregatable(colIdToToggle) && grouping.length > 0 && !visibleColumnIds.includes(colIdToToggle)) {
        shouldRefresh = true
      }
      setColumnVisibility({
        ...columnVisibility,
        [colIdToToggle]: !columnVisibility[colIdToToggle],
      })
      if (shouldRefresh) {
        setRowsToFetch(pendingDataForAllVisibleData(expanded, data, pageSize, pageIndex * pageSize))
      }
    },
    [columnVisibility],
  )

  const onGroupingChange = useCallback(
    (newGroups: ColumnId[]) => {
      // Reset currently expanded/selected when grouping changes
      setSelectedRows({})
      setSidebarJobId(undefined)
      setExpanded({})

      const baseColumnVisibility = {
        ...columnVisibility,
        [StandardColumnId.Count]: false,
      }
      if (newGroups.length > 0) {
        baseColumnVisibility[StandardColumnId.Count] = true
      }
      // Check all grouping columns are displayed
      setColumnVisibility(newGroups.reduce((a, s) => ({ ...a, [s]: true }), baseColumnVisibility))

      setGrouping([...newGroups])

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
      setSidebarJobId(undefined)
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
      setSidebarJobId(undefined)
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

  const onJobRowClick = useCallback((jobRow: JobRow) => {
    const clickedJob = jobRow as Job
    const jobId = clickedJob.jobId
    // Deselect if clicking on a job row that's already shown in the sidebar
    setSidebarJobId(jobId === sidebarJobId ? undefined : jobId)
  }, [])
  const onSideBarClose = useCallback(() => setSidebarJobId(undefined), [])

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
  let columnsForSelect = allColumns
  if (grouping.length === 0) {
    columnsForSelect = columnsForSelect.filter((col) => col.id !== StandardColumnId.Count)
  }
  return (
    <Box sx={{ display: "flex" }}>
      <Box sx={{ overflowX: "auto", overflowY: "auto", margin: "0.5em" }}>
        <JobsTableActionBar
          isLoading={rowsToFetch.length > 0}
          allColumns={columnsForSelect}
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
          <Table sx={{ tableLayout: "fixed" }} aria-label="Jobs table">
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
              sidebarJobId={sidebarJobId}
              onLoadMoreSubRows={onLoadMoreSubRows}
              onClickJobRow={onJobRowClick}
            />

            <TableFooter>
              <TableRow>
                <TablePagination
                  rowsPerPageOptions={PAGE_SIZE_OPTIONS}
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

      {sidebarJobDetails !== undefined && (
        <Sidebar job={sidebarJobDetails} runErrorService={runErrorService} onClose={onSideBarClose} />
      )}
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
