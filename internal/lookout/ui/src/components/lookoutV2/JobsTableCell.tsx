import { RefObject } from "react"

import { KeyboardArrowRight, KeyboardArrowDown } from "@mui/icons-material"
import { TableCell, IconButton, TableSortLabel, Box } from "@mui/material"
import { Cell, ColumnResizeMode, flexRender, Header, Row } from "@tanstack/react-table"
import { JobRow, JobTableRow } from "models/jobsTableModels"
import { Match } from "models/lookoutV2Models"
import { getColumnMetadata, toColId } from "utils/jobsTableColumns"

import styles from "./JobsTableCell.module.css"
import { JobsTableFilter } from "./JobsTableFilter"
import { matchForColumn } from "../../utils/jobsTableUtils"

const sharedCellStyle = {
  padding: 0,
  "&:hover": {
    opacity: 0.85,
  },
  overflowWrap: "normal",
  textOverflow: "ellipsis",
  whiteSpace: "nowrap",
  overflow: "hidden",
  borderRight: "1px solid #cccccc",
}

export interface HeaderCellProps {
  header: Header<JobRow, unknown>
  columnResizeMode: ColumnResizeMode
  deltaOffset: number
  columnMatches: Record<string, Match>
  parseError: string | undefined
  onColumnMatchChange: (columnId: string, newMatch: Match) => void
  onSetTextFieldRef: (ref: RefObject<HTMLInputElement>) => void
}

export function HeaderCell({
  header,
  columnResizeMode,
  deltaOffset,
  columnMatches,
  parseError,
  onColumnMatchChange,
  onSetTextFieldRef,
}: HeaderCellProps) {
  const id = toColId(header.id)
  const columnDef = header.column.columnDef

  const metadata = getColumnMetadata(columnDef)
  const isRightAligned = metadata.isRightAligned ?? false

  const sortDirection = header.column.getIsSorted()
  const defaultSortDirection = "asc"

  const totalWidth = header.column.getSize()
  const resizerWidth = 5
  const borderWidth = 1
  const remainingWidth = totalWidth - resizerWidth - borderWidth

  const match = matchForColumn(header.id, columnMatches)
  if (header.isPlaceholder) {
    return (
      <TableCell
        key={id}
        align={isRightAligned ? "right" : "left"}
        aria-label={metadata.displayName}
        sx={{ ...sharedCellStyle }}
        style={{
          width: `${totalWidth}px`,
          height: 50,
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
          overflow: "hidden",
        }}
        className={styles.headerCell}
      />
    )
  }

  return (
    <TableCell
      key={id}
      align={isRightAligned ? "right" : "left"}
      aria-label={metadata.displayName}
      sx={{ ...sharedCellStyle }}
      style={{
        width: `${totalWidth}px`,
        height: 65,
        textOverflow: "ellipsis",
        whiteSpace: "nowrap",
        overflow: "hidden",
      }}
      className={styles.headerCell}
    >
      <div
        style={{
          height: "100%",
          width: "100%",
          display: "flex",
          flexDirection: "row",
          justifyContent: "space-between",
          alignItems: "center",
          margin: 0,
        }}
      >
        <div
          style={{
            display: "flex",
            flexDirection: "column",
            width: `${remainingWidth}px`,
            padding: "0 10px 0 10px",
          }}
        >
          {header.column.getCanSort() ? (
            <div
              style={{
                display: "flex",
                flexDirection: "row",
                textOverflow: "ellipsis",
                whiteSpace: "nowrap",
                overflow: "hidden",
              }}
            >
              <TableSortLabel
                active={Boolean(sortDirection)}
                direction={sortDirection || defaultSortDirection}
                onClick={() => {
                  const desc = sortDirection ? sortDirection === "asc" : false
                  header.column.toggleSorting(desc)
                }}
                aria-label={"Toggle sort"}
                sx={{
                  width: "100%",
                }}
              >
                <div
                  style={{
                    textOverflow: "ellipsis",
                    whiteSpace: "nowrap",
                    overflow: "hidden",
                  }}
                >
                  {flexRender(columnDef.header, header.getContext())}
                </div>
              </TableSortLabel>
            </div>
          ) : (
            <div
              style={{
                textOverflow: "ellipsis",
                whiteSpace: "nowrap",
                overflow: "hidden",
              }}
            >
              {flexRender(columnDef.header, header.getContext())}
            </div>
          )}

          {header.column.getCanFilter() && metadata.filterType && (
            <JobsTableFilter
              id={header.id}
              currentFilter={header.column.getFilterValue() as string | string[] | number}
              filterType={metadata.filterType}
              matchType={match}
              enumFilterValues={metadata.enumFilterValues}
              parseError={parseError}
              onFilterChange={(val) => header.column.setFilterValue(val)}
              onColumnMatchChange={onColumnMatchChange}
              onSetTextFieldRef={onSetTextFieldRef}
            />
          )}
        </div>
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
      </div>
    </TableCell>
  )
}

export interface BodyCellProps {
  cell: Cell<JobRow, unknown>
  rowIsGroup: boolean
  rowIsExpanded: boolean
  onExpandedChange: () => void
  onClickRowCheckbox: (row: Row<JobTableRow>) => void
}
export const BodyCell = ({ cell, rowIsGroup, rowIsExpanded, onExpandedChange, onClickRowCheckbox }: BodyCellProps) => {
  const columnMetadata = getColumnMetadata(cell.column.columnDef)
  const cellHasValue = cell.renderValue()
  const isRightAligned = columnMetadata.isRightAligned ?? false
  return (
    <TableCell
      key={cell.id}
      align={isRightAligned ? "right" : "left"}
      sx={{
        ...sharedCellStyle,
        padding: "2px 8px 2px 8px",
      }}
    >
      {rowIsGroup && cell.column.getIsGrouped() && cellHasValue ? (
        // If it's a grouped cell, add an expander and row count
        <Box
          sx={{
            display: "flex",
            gap: "0.25em",
          }}
        >
          <IconButton
            size="small"
            sx={{ padding: 0 }}
            edge="start"
            onClick={(e) => {
              onExpandedChange()
              e.stopPropagation()
            }}
          >
            {rowIsExpanded ? (
              <KeyboardArrowDown fontSize="small" aria-label="Collapse row" aria-hidden="false" />
            ) : (
              <KeyboardArrowRight fontSize="small" aria-label="Expand row" aria-hidden="false" />
            )}
          </IconButton>
          <div
            style={{
              overflowWrap: "normal",
              textOverflow: "ellipsis",
              whiteSpace: "nowrap",
              overflow: "hidden",
            }}
          >
            {flexRender(cell.column.columnDef.cell, cell.getContext())}
          </div>
        </Box>
      ) : cell.getIsAggregated() ? (
        // If the cell is aggregated, use the Aggregated
        // renderer for cell
        flexRender(cell.column.columnDef.aggregatedCell ?? cell.column.columnDef.cell, {
          ...cell.getContext(),
          onClickRowCheckbox,
        })
      ) : (
        flexRender(cell.column.columnDef.cell, {
          ...cell.getContext(),
          onClickRowCheckbox,
        })
      )}
    </TableCell>
  )
}
