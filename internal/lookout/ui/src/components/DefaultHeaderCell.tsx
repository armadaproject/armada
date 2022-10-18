import React from "react"

import { TableHeaderProps } from "react-virtualized"

import "./DefaultHeaderCell.css"

type DefaultHeaderCellProps = {
  name: string
  className: string
  dataKey: string
} & TableHeaderProps

export default function DefaultHeaderCell(props: DefaultHeaderCellProps) {
  return (
    <div className={"default-header-cell " + props.className}>
      <div>{props.name}</div>
    </div>
  )
}
