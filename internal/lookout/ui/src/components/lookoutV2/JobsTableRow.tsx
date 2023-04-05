import React, { useState, MouseEvent } from "react"

import { TableRow } from "@mui/material"
import { Row } from "@tanstack/table-core"
import { JobTableRow, JobRow, isJobGroupRow } from "models/jobsTableModels"

import { BodyCell } from "./JobsTableCell"
import styles from "./JobsTableRow.module.css"

export interface JobsTableRowProps {
  row: Row<JobTableRow>
  isOpenInSidebar: boolean
  onClick?: (row: JobRow, e: MouseEvent<HTMLTableRowElement>) => void
}
export const JobsTableRow = ({ row, isOpenInSidebar, onClick }: JobsTableRowProps) => {
  // Helpers to avoid triggering onClick if the user is selecting text
  const [{ pageX, pageY }, setPagePosition] = useState({ pageX: -1, pageY: -1 })
  const isDragging = (e: React.MouseEvent) => {
    const dragThresholdPixels = 6
    const diffX = Math.abs(e.pageX - pageX)
    const diffY = Math.abs(e.pageY - pageY)
    return diffX > dragThresholdPixels || diffY > dragThresholdPixels
  }

  const original = row.original
  const rowIsGroup = isJobGroupRow(original)
  const rowCells = row.getVisibleCells()
  return (
    <TableRow
      aria-label={row.id}
      className={styles.rowDepthIndicator}
      sx={{
        backgroundSize: row.depth * 6,

        ...(onClick && {
          cursor: "pointer",
        }),

        ...(isOpenInSidebar && {
          backgroundColor: "rgb(51, 187, 231, 0.5)",
        }),

        "&:hover": {
          backgroundColor: "rgb(51, 187, 231, 0.2)",
        },
      }}
      onMouseDown={(e) => setPagePosition(e)}
      onClick={(e) => {
        if (!isDragging(e) && onClick) {
          onClick(row.original, e)
        }
      }}
    >
      {rowCells.map((cell) => (
        <BodyCell
          cell={cell}
          rowIsGroup={rowIsGroup}
          rowIsExpanded={row.getIsExpanded()}
          onExpandedChange={row.toggleExpanded}
          key={cell.id}
        />
      ))}
    </TableRow>
  )
}
