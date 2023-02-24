import { KeyboardArrowRight, KeyboardArrowDown } from "@mui/icons-material"
import { TableCell, IconButton, TableSortLabel, Box } from "@mui/material"
import { Cell, ColumnResizeMode, flexRender, Header } from "@tanstack/react-table"
import { JobRow } from "models/jobsTableModels"
import { Match } from "models/lookoutV2Models"
import { getColumnMetadata, toColId } from "utils/jobsTableColumns"

import styles from "./JobsTableCell.module.css"
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
  columnResizeMode: ColumnResizeMode
  deltaOffset: number
}
export const HeaderCell = ({ header, columnResizeMode, deltaOffset }: HeaderCellProps) => {
  const id = toColId(header.id)
  const columnDef = header.column.columnDef

  const metadata = getColumnMetadata(columnDef)
  const isRightAligned = metadata.isRightAligned ?? false

  const sortDirection = header.column.getIsSorted()
  const defaultSortDirection = "asc"

  return (
    <TableCell
      key={id}
      align={isRightAligned ? "right" : "left"}
      aria-label={metadata.displayName}
      sx={{ ...sharedCellStyle }}
      style={{
        width: `${header.column.getSize()}px`,
        height: "100%",
      }}
    >
      <div
        style={{
          width: "100%",
          height: "100%",
          display: "flex",
          flexDirection: "row",
          justifyContent: "space-between",
        }}
      >
        <div
          style={{
            display: "flex",
            flexDirection: "column",
          }}
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
            flexRender(columnDef.header, header.getContext())
          )}

          {header.column.getCanFilter() && metadata.filterType && (
            <JobsTableFilter
              id={header.id}
              currentFilter={header.column.getFilterValue() as string | string[]}
              filterType={metadata.filterType}
              matchType={metadata.defaultMatchType ?? Match.Exact}
              enumFilterValues={metadata.enumFitlerValues}
              onFilterChange={header.column.setFilterValue}
            />
          )}
        </div>
        {!header.isPlaceholder && (
          <div
            {...{
              onMouseDown: header.getResizeHandler(),
              onTouchStart: header.getResizeHandler(),
              className: (header.column.getIsResizing() ? [styles.resizer, styles.isResizing] : [styles.resizer]).join(
                " ",
              ),
              style: {
                transform:
                  columnResizeMode === "onEnd" && header.column.getIsResizing() ? `translateX(${deltaOffset}px)` : "",
              },
            }}
          />
        )}
      </div>
    </TableCell>
  )
}

export interface BodyCellProps {
  cell: Cell<JobRow, unknown>
  rowIsGroup: boolean
  rowIsExpanded: boolean
  onExpandedChange: () => void
}
export const BodyCell = ({ cell, rowIsGroup, rowIsExpanded, onExpandedChange }: BodyCellProps) => {
  const columnMetadata = getColumnMetadata(cell.column.columnDef)
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
        <Box sx={{ display: "flex", gap: "0.25em" }}>
          <IconButton size="small" sx={{ padding: 0 }} edge="start" onClick={() => onExpandedChange()}>
            {rowIsExpanded ? (
              <KeyboardArrowDown fontSize="small" aria-label="Collapse row" aria-hidden="false" />
            ) : (
              <KeyboardArrowRight fontSize="small" aria-label="Expand row" aria-hidden="false" />
            )}
          </IconButton>
          {flexRender(cell.column.columnDef.cell, cell.getContext())}
        </Box>
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
