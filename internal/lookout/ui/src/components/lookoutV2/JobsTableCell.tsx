import { KeyboardArrowRight, KeyboardArrowDown } from "@mui/icons-material"
import { TableCell, IconButton, TableSortLabel } from "@mui/material"
import { Cell, flexRender, Header } from "@tanstack/react-table"
import { JobRow } from "models/jobsTableModels"
import { ColumnId, getColumnMetadata } from "utils/jobsTableColumns"

import { JobsTableFilter } from "./JobsTableFilter"

const sharedCellStyle = {
  padding: "0.5em",
  overflowWrap: "break-word",
  "&:hover": {
    opacity: 0.85,
  },
}

export interface HeaderCellProps {
  header: Header<JobRow, unknown>
}
export const HeaderCell = ({ header }: HeaderCellProps) => {
  const id = header.id as ColumnId
  const columnDef = header.column.columnDef

  const metadata = getColumnMetadata(columnDef)
  const isRightAligned = metadata.isRightAligned ?? false

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
      aria-label={metadata.displayName}
    >
      {header.isPlaceholder ? null : header.column.getCanSort() ? (
        <TableSortLabel
          active={Boolean(sortDirection)}
          direction={sortDirection || defaultSortDirection}
          onClick={() => {
            const desc = sortDirection ? sortDirection === "asc" : false
            header.column.toggleSorting(desc)
          }}
          aria-label={"Toggle sort"}
        >
          {flexRender(columnDef.header, header.getContext())}
          {header.column.getIsGrouped() && <> (# Jobs)</>}
        </TableSortLabel>
      ) : (
        <>
          {flexRender(columnDef.header, header.getContext())}
          {header.column.getIsGrouped() && <> (# Jobs)</>}
        </>
      )}

      {header.column.getCanFilter() && metadata.filterType && (
        <JobsTableFilter
          id={header.id}
          currentFilter={header.column.getFilterValue() as string | string[]}
          filterType={metadata.filterType}
          enumFilterValues={metadata.enumFitlerValues}
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
  const columnMetadata = getColumnMetadata(cell.column)
  const cellHasValue = cell.renderValue()
  const isRightAligned = columnMetadata.isRightAligned ?? false
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
