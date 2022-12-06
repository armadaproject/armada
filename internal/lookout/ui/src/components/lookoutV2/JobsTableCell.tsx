import { KeyboardArrowRight, KeyboardArrowDown } from "@mui/icons-material"
import { TableCell, IconButton, TableSortLabel } from "@mui/material"
import { Cell, flexRender, Header } from "@tanstack/react-table"
import _ from "lodash"
import { JobRow } from "models/jobsTableModels"
import { ColumnId, ColumnSpec, columnSpecFor } from "utils/jobsTableColumns"
import { JobsTableFilter } from "./JobsTableFilter"

const sharedCellStyle = {
  padding: "0.5em",
  "&:hover": {
    opacity: 0.85,
  },
}

const shouldRightAlign = (colSpec: ColumnSpec): boolean => Boolean(colSpec.isNumeric)

export interface HeaderCellProps {
  header: Header<JobRow, unknown>
}
export const HeaderCell = ({ header }: HeaderCellProps) => {
  const id = header.id as ColumnId
  const colSpec = columnSpecFor(id)
  const isRightAligned = shouldRightAlign(colSpec)
  const sortDirection = header.column.getIsSorted()
  const defaultSortDirection = "asc"

  return (
    <TableCell
      key={id}
      align={isRightAligned ? "right" : "left"}
      sx={{
        width: `${header.column.getSize()}px`,
        ...sharedCellStyle,
      }}
      aria-label={colSpec.name}
    >
      {header.isPlaceholder ? null : (
        <TableSortLabel
          active={Boolean(sortDirection)}
          direction={sortDirection || defaultSortDirection}
          onClick={() => {
            const desc = sortDirection ? sortDirection === "asc" : false
            header.column.toggleSorting(desc)
          }}
          hideSortIcon={!header.column.getCanSort()}
          aria-label={"Toggle sort"}
          disabled={!header.column.getCanSort()}
        >
          {flexRender(header.column.columnDef.header, header.getContext())}
          {header.column.getIsGrouped() && <> (# Jobs)</>}
        </TableSortLabel>
      )}

      {header.column.getCanFilter() && colSpec.filterType && (
        <JobsTableFilter
          id={header.id}
          currentFilter={header.column.getFilterValue() as string | string[]}
          filterType={colSpec.filterType}
          enumFilterValues={colSpec.enumFitlerValues}
          onFilterChange={header.column.setFilterValue}
        />
      )}
    </TableCell>
  )
}

export interface BodyCellProps {
  cell: Cell<JobRow, unknown>
  rowIsGroup: boolean
  rowIsExpanded: boolean
  onExpandedChange: () => void
  subCount: number | undefined
}
export const BodyCell = ({ cell, rowIsGroup, rowIsExpanded, onExpandedChange, subCount }: BodyCellProps) => {
  const colId = cell.column.id as ColumnId
  const colSpec = columnSpecFor(colId)
  const cellHasValue = cell.renderValue()
  const isRightAligned = shouldRightAlign(colSpec)
  return (
    <TableCell
      key={cell.id}
      align={isRightAligned ? "right" : "left"}
      sx={{
        ...sharedCellStyle,
      }}
    >
      {rowIsGroup && cell.column.getIsGrouped() && cellHasValue ? (
        // If it's a grouped cell, add an expander and row count
        <>
          <IconButton size="small" sx={{ padding: 0 }} edge="start" onClick={() => onExpandedChange()}>
            {rowIsExpanded ? (
              <KeyboardArrowDown fontSize="small" aria-label="Collapse row" aria-hidden="false" />
            ) : (
              <KeyboardArrowRight fontSize="small" aria-label="Expand row" aria-hidden="false" />
            )}
          </IconButton>
          {flexRender(cell.column.columnDef.cell, cell.getContext())} ({subCount})
        </>
      ) : cell.getIsAggregated() ? (
        // If the cell is aggregated, use the Aggregated
        // renderer for cell
        flexRender(cell.column.columnDef.aggregatedCell ?? cell.column.columnDef.cell, cell.getContext())
      ) : (
        flexRender(cell.column.columnDef.cell, cell.getContext())
      )}
    </TableCell>
  )
}
