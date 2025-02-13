import { ReactNode, RefObject, useCallback } from "react"

import { KeyboardArrowRight, KeyboardArrowDown } from "@mui/icons-material"
import { TableCell, IconButton, TableSortLabel, Box, styled } from "@mui/material"
import { Cell, ColumnResizeMode, flexRender, Header, Row } from "@tanstack/react-table"

import styles from "./JobsTableCell.module.css"
import { JobsTableFilter } from "./JobsTableFilter"
import { JobRow, JobTableRow } from "../../models/jobsTableModels"
import { JobState, Match } from "../../models/lookoutModels"
import { ColumnId, FilterType, getColumnMetadata, StandardColumnId, toColId } from "../../utils/jobsTableColumns"
import { matchForColumn } from "../../utils/jobsTableUtils"
import { ActionableValueOnHover } from "../ActionableValueOnHover"
import { JobGroupStateCountsColumnHeader } from "./JobGroupStateCountsColumnHeader"

const sharedCellStyle = {
  padding: 0,
  overflowWrap: "normal",
  textOverflow: "ellipsis",
  whiteSpace: "nowrap",
  overflow: "hidden",
  borderRight: "1px solid #cccccc",
} as const

const sharedCellStyleWithOpacity = {
  ...sharedCellStyle,
  "&:hover": {
    opacity: 0.85,
  },
} as const

const HeaderTableCell = styled(TableCell)(({ theme }) => ({
  backgroundColor: theme.palette.grey[200],
  ...theme.applyStyles("dark", {
    backgroundColor: theme.palette.grey[800],
  }),
}))

const JobGroupStateCountsColumnHeaderContainer = styled("div")({
  marginTop: 5,
  // Body cells have 8px horizontal padding while header cells have 10px. -2px margin on this container adjusts for this
  marginLeft: -2,
  marginRight: -2 - 5, // -5px to adjust for the column resizer
})

export interface HeaderCellProps {
  header: Header<JobRow, unknown>
  columnResizeMode: ColumnResizeMode
  deltaOffset: number
  columnMatches: Record<string, Match>
  parseError: string | undefined
  onColumnMatchChange: (columnId: string, newMatch: Match) => void
  onSetTextFieldRef: (ref: RefObject<HTMLInputElement>) => void
  groupedColumns: ColumnId[]
}

export function HeaderCell({
  header,
  columnResizeMode,
  deltaOffset,
  columnMatches,
  parseError,
  onColumnMatchChange,
  onSetTextFieldRef,
  groupedColumns,
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

  const onFilterChange = useCallback(
    (newFilter: string | string[] | number | undefined) => header.column.setFilterValue(newFilter),
    [header.column.setFilterValue],
  )

  const match = matchForColumn(header.id, columnMatches)
  if (header.isPlaceholder) {
    return (
      <HeaderTableCell
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
      />
    )
  }

  return (
    <HeaderTableCell
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
              onFilterChange={onFilterChange}
              onColumnMatchChange={onColumnMatchChange}
              onSetTextFieldRef={onSetTextFieldRef}
            />
          )}

          {header.column.id === StandardColumnId.State &&
            groupedColumns.filter((id) => id !== StandardColumnId.State)?.length > 0 && (
              <JobGroupStateCountsColumnHeaderContainer>
                <JobGroupStateCountsColumnHeader
                  jobStatesToDisplay={header.column.getFilterValue() as JobState[] | undefined}
                />
              </JobGroupStateCountsColumnHeaderContainer>
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
    </HeaderTableCell>
  )
}

const BodyTableCell = styled(TableCell)({
  ...sharedCellStyleWithOpacity,
  padding: "2px 8px 2px 8px",
})

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

  const cellContent = ((): ReactNode => {
    // If it's a grouped cell, add an expander and row count
    if (rowIsGroup && cell.column.getIsGrouped() && cellHasValue) {
      return (
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
      )
    }

    // If the cell is aggregated, use the Aggregated renderer for cell
    if (cell.getIsAggregated()) {
      return flexRender(cell.column.columnDef.aggregatedCell ?? cell.column.columnDef.cell, {
        ...cell.getContext(),
        onClickRowCheckbox,
      })
    }

    return (
      <ActionableValueOnHover
        stopPropogationOnActionClick
        copyAction={
          !rowIsGroup && Boolean(cell.getValue()) && columnMetadata.allowCopy
            ? { copyContent: String(cell.getValue()) }
            : undefined
        }
        filterAction={
          rowIsGroup || !cell.getValue() || columnMetadata.filterType === undefined
            ? undefined
            : {
                onFilter: () => {
                  cell.column.setFilterValue(
                    columnMetadata.filterType === FilterType.Enum ? [cell.getValue()] : cell.getValue(),
                  )
                },
              }
        }
      >
        {flexRender(cell.column.columnDef.cell, {
          ...cell.getContext(),
          onClickRowCheckbox,
        })}
      </ActionableValueOnHover>
    )
  })()

  return (
    <BodyTableCell key={cell.id} align={isRightAligned ? "right" : "left"}>
      {cellContent}
    </BodyTableCell>
  )
}
