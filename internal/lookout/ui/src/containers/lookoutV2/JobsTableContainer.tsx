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
} from "@tanstack/react-table"
import { JobsTableActionBar } from "components/lookoutV2/JobsTableActionBar"
import { BodyCell, HeaderCell } from "components/lookoutV2/JobsTableCell"
import { getSelectedColumnDef, SELECT_COLUMN_ID } from "components/lookoutV2/SelectedColumn"
import _ from "lodash"
import { JobTableRow, isJobGroupRow, JobRow, JobGroupRow } from "models/jobsTableModels"
import { JobFilter } from "models/lookoutV2Models"
import { CancelJobsService } from "services/lookoutV2/CancelJobsService"
import GetJobsService from "services/lookoutV2/GetJobsService"
import GroupJobsService from "services/lookoutV2/GroupJobsService"
import { ColumnId, DEFAULT_COLUMN_SPECS, DEFAULT_GROUPING } from "utils/jobsTableColumns"
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

const DEFAULT_PAGE_SIZE = 30

interface JobsTableContainerProps {
  getJobsService: GetJobsService
  groupJobsService: GroupJobsService
  cancelJobsService: CancelJobsService
  debug: boolean
}
export const JobsTableContainer = ({
  getJobsService,
  groupJobsService,
  cancelJobsService,
  debug,
}: JobsTableContainerProps) => {
  // Data
  const [isLoading, setIsLoading] = useState(true)
  const [data, setData] = useState<JobTableRow[]>([])
  const [rowsToFetch, setRowsToFetch] = useState<PendingData[]>([{ parentRowId: "ROOT", skip: 0 }])
  const [totalRowCount, setTotalRowCount] = useState(0)
  const [allColumns, setAllColumns] = useState(DEFAULT_COLUMN_SPECS)

  // Grouping
  const [grouping, setGrouping] = useState<ColumnId[]>(DEFAULT_GROUPING)

  // Expanding
  const [expanded, setExpanded] = useState<ExpandedStateList>({})

  // Selecting
  const [selectedRows, setSelectedRows] = useState<RowSelectionState>({})

  // Pagination
  const [pagination, setPagination] = useState<PaginationState>({
    pageIndex: 0,
    pageSize: DEFAULT_PAGE_SIZE,
  })
  const [pageCount, setPageCount] = useState<number>(-1)
  const { pageIndex, pageSize } = useMemo(() => pagination, [pagination])

  // Filtering
  const [columnFilterState, setColumnFilterState] = useState<ColumnFiltersState>([])

  // Sorting
  const [sorting, setSorting] = useState<SortingState>([{ id: "jobId", desc: true }])

  useEffect(() => {
    async function fetchData() {
      if (rowsToFetch.length === 0) {
        return
      }

      const [nextRequest, ...restOfRequests] = rowsToFetch

      const parentRowInfo = nextRequest.parentRowId !== "ROOT" ? fromRowId(nextRequest.parentRowId) : undefined

      const groupingLevel = grouping.length
      const expandedLevel = parentRowInfo ? parentRowInfo.rowIdPathFromRoot.length : 0
      const isJobFetch = expandedLevel === groupingLevel

      const sortedField = sorting[0]

      const rowRequest: FetchRowRequest = {
        filters: [
          ...convertRowPartsToFilters(parentRowInfo?.rowIdPartsPath ?? []),
          ...convertColumnFiltersToFilters(columnFilterState),
        ],
        skip: nextRequest.skip ?? 0,
        take: nextRequest.take ?? pageSize,
        order: { field: sortedField.id, direction: sortedField.desc ? "DESC" : "ASC" },
      }

      let newData, totalCount
      if (isJobFetch) {
        const { jobs, totalJobs } = await fetchJobs(rowRequest, getJobsService)
        newData = jobsToRows(jobs)
        totalCount = totalJobs
      } else {
        const groupedCol = grouping[expandedLevel]
        const colsToAggregate = allColumns.filter((c) => c.groupable).map((c) => c.key)
        const { groups, totalGroups } = await fetchJobGroups(rowRequest, groupJobsService, groupedCol, colsToAggregate)
        newData = groupsToRows(groups, parentRowInfo?.rowId, groupedCol)
        totalCount = totalGroups
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
          setSelectedRows(newSelectedRows)
        }
      }

      setData([...rootData]) // ReactTable will only re-render if the array identity changes
      setIsLoading(false)
      setRowsToFetch(restOfRequests)
      if (parentRowInfo === undefined) {
        setPageCount(Math.ceil(totalCount / pageSize))
        setTotalRowCount(totalCount)
      }
    }

    fetchData().catch(console.error)
  }, [rowsToFetch, pagination, grouping, expanded, columnFilterState, sorting])

  const onGroupingChange = useCallback(
    (newState: ColumnId[]) => {
      // Reset currently expanded/selected when grouping changes
      setSelectedRows({})
      setExpanded({})

      // Check all grouping columns are displayed
      setAllColumns(
        allColumns.map((col) => ({
          ...col,
          selected: newState.includes(col.key) ? true : col.selected,
        })),
      )

      setGrouping([...newState])

      // Refetch the root data
      setRowsToFetch([{ parentRowId: "ROOT", skip: 0 }])
    },
    [allColumns],
  )

  const onRootPaginationChange = useCallback(
    (updater: Updater<PaginationState>) => {
      const newPagination = updaterToValue(updater, pagination)
      // Reset currently expanded/selected when grouping changes
      // TODO: Consider allowing rows to be selected across pages?
      setSelectedRows({})
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

  const selectedItemsFilters: JobFilter[][] = useMemo(() => {
    const tableFilters = convertColumnFiltersToFilters(columnFilterState)
    return Object.keys(selectedRows).map((rowId) => {
      const { rowIdPartsPath } = fromRowId(rowId as RowId)
      return tableFilters.concat(convertRowPartsToFilters(rowIdPartsPath))
    })
  }, [selectedRows, columnFilterState])

  const selectedColumnDefs = useMemo<ColumnDef<JobTableRow>[]>(() => {
    return allColumns
      .filter((c) => c.selected)
      .map(
        (c): ColumnDef<JobTableRow> => ({
          id: c.key,
          accessorKey: c.key,
          header: c.name,
          enableGrouping: c.groupable,
          enableColumnFilter: c.filterType !== undefined,
          enableSorting: c.sortable,
          aggregationFn: () => "-",
          minSize: c.minSize,
          size: c.minSize,
          ...(c.formatter ? { cell: (info) => c.formatter?.(info.getValue()) } : {}),
        }),
      )
      .concat([getSelectedColumnDef()])
  }, [allColumns])

  const table = useReactTable({
    data: data ?? [],
    columns: selectedColumnDefs,
    state: {
      grouping,
      expanded,
      pagination,
      columnFilters: columnFilterState,
      rowSelection: selectedRows,
      columnPinning: {
        left: [SELECT_COLUMN_ID],
      },
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
    <div className={styles.jobsTablePage}>
      <JobsTableActionBar
        allColumns={allColumns}
        groupedColumns={grouping}
        selectedItemFilters={selectedItemsFilters}
        onColumnsChanged={setAllColumns}
        onGroupsChanged={onGroupingChange}
        getJobsService={getJobsService}
        cancelJobsService={cancelJobsService}
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
            dataIsLoading={isLoading}
            columns={selectedColumnDefs}
            topLevelRows={topLevelRows}
            onLoadMoreSubRows={onLoadMoreSubRows}
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
                colSpan={selectedColumnDefs.length}
              />
            </TableRow>
          </TableFooter>
        </Table>
      </TableContainer>

      {debug && <pre>{JSON.stringify(table.getState(), null, 2)}</pre>}
    </div>
  )
}

interface JobsTableBodyProps {
  dataIsLoading: boolean
  columns: ColumnDef<JobTableRow>[]
  topLevelRows: Row<JobTableRow>[]
  onLoadMoreSubRows: (rowId: RowId, skip: number) => void
}
const JobsTableBody = ({ dataIsLoading, columns, topLevelRows, onLoadMoreSubRows }: JobsTableBodyProps) => {
  const canDisplay = !dataIsLoading && topLevelRows.length > 0
  return (
    <TableBody>
      {!canDisplay && (
        <TableRow>
          {dataIsLoading && (
            <TableCell colSpan={columns.length}>
              <CircularProgress />
            </TableCell>
          )}
          {!dataIsLoading && topLevelRows.length === 0 && (
            <TableCell colSpan={columns.length}>There is no data to display</TableCell>
          )}
        </TableRow>
      )}

      {topLevelRows.map((row) => recursiveRowRender(row, onLoadMoreSubRows))}
    </TableBody>
  )
}

const recursiveRowRender = (
  row: Row<JobTableRow>,
  onLoadMoreSubRows: (rowId: RowId, skip: number) => void,
): JSX.Element => {
  const original = row.original
  const rowIsGroup = isJobGroupRow(original)
  const rowCells = row.getVisibleCells()

  const depthGaugeLevelThicknessPixels = 6

  return (
    <React.Fragment key={`${row.id}_d${row.depth}`}>
      {/* Render the current row */}
      <TableRow aria-label={row.id} hover className={styles.rowDepthIndicator} sx={{ backgroundSize: row.depth * 6 }}>
        {rowCells.map((cell) => (
          <BodyCell
            cell={cell}
            rowIsGroup={rowIsGroup}
            rowIsExpanded={row.getIsExpanded()}
            onExpandedChange={row.toggleExpanded}
            subCount={rowIsGroup ? original.jobCount : undefined}
            key={cell.id}
          />
        ))}
      </TableRow>

      {/* Render any sub rows if expanded */}
      {rowIsGroup && row.getIsExpanded() && row.subRows.map((row) => recursiveRowRender(row, onLoadMoreSubRows))}

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
