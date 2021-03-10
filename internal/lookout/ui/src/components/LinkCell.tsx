import React from "react"
import { TableCellProps } from "react-virtualized"

import "./LinkCell.css"

type JobIdCellProps = {
  onClick: () => void
} & TableCellProps

export default function LinkCell(props: JobIdCellProps) {
  return (
    <div className="link" onClick={props.onClick}>
      {props.cellData}
    </div>
  )
}
