import React from "react"

import { TableCellProps } from "react-virtualized"

import "./LinkCell.css"

type LinkCellProps = {
  onClick: () => void
} & TableCellProps

export default function LinkCell(props: LinkCellProps) {
  return (
    <div className="link" onClick={props.onClick}>
      {props.cellData}
    </div>
  )
}
