import React from "react"

import { TableRowProps } from "react-virtualized"

import "../Row.css"

export default function LoadingRow(props: TableRowProps) {
  return (
    <div className={"loading-row " + props.className} style={props.style} key="LOADING">
      Loading...
    </div>
  )
}
