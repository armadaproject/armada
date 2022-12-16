import React from "react"

import { green, grey, orange, purple, red, yellow } from "@material-ui/core/colors"
import { TableCellProps } from "react-virtualized"

import "./JobStateCell.css"

function colorForState(state: string): string | undefined {
  switch (state) {
    case "Queued":
      return yellow["A100"]
    case "Pending":
      return orange["A100"]
    case "Running":
      return green["A100"]
    case "Succeeded":
      return undefined
    case "Failed":
      return red["A100"]
    case "Cancelled":
      return grey[300]
    default:
      return purple["A100"]
  }
}

export default function JobStateCell(props: Pick<TableCellProps, "cellData">) {
  return (
    <div
      className="job-state-cell"
      style={{
        backgroundColor: colorForState(props.cellData),
      }}
    >
      {props.cellData}
    </div>
  )
}
