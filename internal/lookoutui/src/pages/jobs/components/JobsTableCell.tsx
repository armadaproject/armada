import { ReactNode, RefObject, useCallback } from "react"

import { KeyboardArrowRight, KeyboardArrowDown, OpenInNew } from "@mui/icons-material"
import { TableCell, IconButton, TableSortLabel, Box, styled, Typography, Link, Tooltip } from "@mui/material"
import { Cell, ColumnResizeMode, flexRender, Header, Row } from "@tanstack/react-table"
import validator from "validator"

import {
  ColumnId,
  FilterType,
  getColumnMetadata,
  StandardColumnId,
  toColId,
  isStandardColId,
  PREREQUISITE_FILTER_COLUMNS,
  STANDARD_COLUMN_DISPLAY_NAMES,
} from "../../../common/jobsTableColumns"
import { matchForColumn } from "../../../common/jobsTableUtils"
import { ActionableValueOnHover } from "../../../components/ActionableValueOnHover"
import { CopyIconButton } from "../../../components/CopyIconButton"
import { JobsTableFilter } from "../../../components/JobsTableFilter"
import { LastTransitionTimeAggregateSelector } from "../../../components/LastTransitionTimeAggregateSelector"
import { JobRow, JobTableRow } from "../../../models/jobsTableModels"
import { AggregateType, JobState, Match } from "../../../models/lookoutModels"

import { JobGroupStateCountsColumnHeader } from "./JobGroupStateCountsColumnHeader"
import styles from "./JobsTableCell.module.css"

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
  onSetTextFieldRef: (ref: RefObject<HTMLInputElement | undefined>) => void
  groupedColumns: ColumnId[]
  lastTransitionTimeAggregate: AggregateType
  onLastTransitionTimeAggregateChange: (value: AggregateType) => void
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
  lastTransitionTimeAggregate,
  onLastTransitionTimeAggregateChange,
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

  const allowCopyColumn = Boolean(metadata.allowCopyColumn)
  const visibleRows = header.getContext().table.getRowModel().rows
  const copyContent = getColumnMetadata(header.column.columnDef).allowCopyColumn
    ? visibleRows
        .map((row) => row.original.jobId)
        .filter(Boolean)
        .join(", ")
    : ""

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

  const prerequisiteFilterColumns = isStandardColId(id) ? PREREQUISITE_FILTER_COLUMNS[id as StandardColumnId] : []
  const prerequisiteFilterColumnsSatisfied = prerequisiteFilterColumns.every((pid) =>
    header
      .getContext()
      .table.getState()
      .columnFilters.some((f) => f.id === pid),
  )

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
                alignItems: "center",
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
                  flex: 1,
                  minWidth: 0,
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

              {allowCopyColumn && copyContent && (
                <Tooltip title="Copy All" arrow>
                  <span style={{ flex: "0 0 auto", display: "flex", alignItems: "center" }}>
                    <CopyIconButton size="small" content={copyContent} onClick={(e) => e.stopPropagation()} />
                  </span>
                </Tooltip>
              )}
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

              {allowCopyColumn && copyContent && (
                <Tooltip title="Copy All" arrow>
                  <span style={{ display: "inline-flex", alignItems: "center", marginLeft: 6 }}>
                    <CopyIconButton size="small" content={copyContent} onClick={(e) => e.stopPropagation()} />
                  </span>
                </Tooltip>
              )}
            </div>
          )}

          {header.column.getCanFilter() &&
            metadata.filterType &&
            (prerequisiteFilterColumnsSatisfied ? (
              <JobsTableFilter
                id={header.id}
                currentFilter={header.column.getFilterValue() as string | string[] | number}
                filterType={metadata.filterType}
                matchType={match}
                enumFilterValues={metadata.enumFilterValues}
                enumFilterCategories={metadata.enumFilterCategories}
                parseError={parseError}
                onFilterChange={onFilterChange}
                onColumnMatchChange={onColumnMatchChange}
                onSetTextFieldRef={onSetTextFieldRef}
              />
            ) : (
              <Typography component="div" variant="body2" color="textSecondary">
                To filter by {metadata.displayName}, add a filter for{" "}
                {prerequisiteFilterColumns
                  .map((pid) => STANDARD_COLUMN_DISPLAY_NAMES[pid])
                  .join(", ")
                  .replace(/, ([^,]*)$/, " and $1")}
              </Typography>
            ))}

          {header.column.id === StandardColumnId.State &&
            groupedColumns.filter((id) => id !== StandardColumnId.State)?.length > 0 && (
              <JobGroupStateCountsColumnHeaderContainer>
                <JobGroupStateCountsColumnHeader
                  jobStatesToDisplay={header.column.getFilterValue() as JobState[] | undefined}
                />
              </JobGroupStateCountsColumnHeaderContainer>
            )}

          {header.column.id === StandardColumnId.TimeInState &&
            Boolean(groupedColumns.find((id) => id !== StandardColumnId.TimeInState)) && (
              <LastTransitionTimeAggregateSelector
                value={lastTransitionTimeAggregate}
                onChange={onLastTransitionTimeAggregateChange}
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
  onColumnMatchChange: (columnId: string, newMatch: Match) => void
}

export const BodyCell = ({
  cell,
  rowIsGroup,
  rowIsExpanded,
  onExpandedChange,
  onClickRowCheckbox,
  onColumnMatchChange,
}: BodyCellProps) => {
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

    const value = cell.getValue()

    const isAnnotation = !isStandardColId(cell.getContext().column.id)
    const renderedValue =
      isAnnotation && typeof value === "string" && validator.isURL(value) ? (
        <Link href={value} target="_blank">
          {flexRender(cell.column.columnDef.cell, {
            ...cell.getContext(),
            onClickRowCheckbox,
          })}
          <OpenInNew fontSize="inherit" />
        </Link>
      ) : (
        <span>
          {flexRender(cell.column.columnDef.cell, {
            ...cell.getContext(),
            onClickRowCheckbox,
          })}
        </span>
      )

    return (
      <ActionableValueOnHover
        stopPropagationOnActionClick
        copyAction={
          !rowIsGroup && Boolean(value) && columnMetadata.allowCopy ? { copyContent: String(value) } : undefined
        }
        filterAction={
          rowIsGroup ||
          !value ||
          (columnMetadata.filterType !== FilterType.Enum && columnMetadata.filterType !== FilterType.Text)
            ? undefined
            : {
                onFilter: () => {
                  if (cell.column.id === StandardColumnId.Queue || columnMetadata.filterType === FilterType.Enum) {
                    cell.column.setFilterValue([value])
                  } else {
                    onColumnMatchChange(cell.column.id, Match.Exact)
                    cell.column.setFilterValue(value)
                  }
                },
              }
        }
      >
        {renderedValue}
      </ActionableValueOnHover>
    )
  })()

  return (
    <BodyTableCell key={cell.id} align={isRightAligned ? "right" : "left"}>
      {cellContent}
    </BodyTableCell>
  )
}
