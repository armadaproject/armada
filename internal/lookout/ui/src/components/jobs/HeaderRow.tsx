import React from "react";
import { TableHeaderRowProps } from "react-virtualized";

import "./Row.css"

export default function HeaderRow(props: TableHeaderRowProps) {
  return (
    <div className={"job-row " + props.className} style={props.style}>
      <div className="select-box" />
      {props.columns}
    </div>
  )
}
