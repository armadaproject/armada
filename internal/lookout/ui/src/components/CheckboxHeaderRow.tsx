import React from "react";
import { TableHeaderRowProps } from "react-virtualized";

import "./Row.css"

export default function CheckboxHeaderRow(props: TableHeaderRowProps) {
  return (
    <div className={props.className} style={props.style}>
      <div className="select-box" />
      {props.columns}
    </div>
  )
}
