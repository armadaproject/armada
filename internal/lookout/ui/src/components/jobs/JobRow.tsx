import React from "react"
import { TableRowProps } from "react-virtualized";
import { Checkbox } from "@material-ui/core";

import "./Row.css"

interface JobRowProps extends TableRowProps {
  isChecked: boolean
  onChangeChecked: (checked: boolean) => void
  tableKey: string
}

export default function JobRow(props: JobRowProps) {
  return (
    <div key={props.tableKey} className={"job-row " + props.className} style={props.style}>
      <div className="select-box">
        <Checkbox color={"secondary"}
          checked={props.isChecked}
          onChange={(event) => props.onChangeChecked(event.target.checked)} />
      </div>
      {props.columns}
    </div>
  )
}
