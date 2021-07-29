import React from "react"

import { TableRowProps } from "react-virtualized"

import "../Row.css"

const EVEN_ROW_COLOR = "#ffffff"
const ODD_ROW_COLOR = "#f5f5f5"

export default function LoadingRow(props: TableRowProps) {
  let color = EVEN_ROW_COLOR
  if (props.index % 2 != 0) {
    color = ODD_ROW_COLOR
  }
  return (
    <div className={"loading-row " + props.className} style={{ ...props.style, background: color }} key="LOADING">
      Loading...
    </div>
  )
}
