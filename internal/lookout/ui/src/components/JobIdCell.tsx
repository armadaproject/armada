import React from "react"
import { TableCellProps } from "react-virtualized"

import "./Cell.css"

type JobIdCellProps = {
  onClick: () => void
} & TableCellProps

export default function JobIdCell(props: JobIdCellProps) {
  return (
    <div className="job-id-cell" onClick={props.onClick}>
      {props.cellData}
    </div>
  )
}
