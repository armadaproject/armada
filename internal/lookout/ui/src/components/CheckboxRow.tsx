import React from "react"

import { Checkbox } from "@material-ui/core"
import { TableRowProps } from "react-virtualized"

import "./Row.css"

type CheckboxRowProps = {
  isChecked: boolean
  onChangeChecked: (checked: boolean) => void
  onChangeCheckedShift: (checked: boolean) => void
  tableKey: string
} & TableRowProps

const EVEN_ROW_COLOR = "#ffffff"
const ODD_ROW_COLOR = "#f5f5f5"
const SELECTED_ROW_COLOR = "#b2ebf2"

export default function CheckboxRow(props: CheckboxRowProps) {
  let color = EVEN_ROW_COLOR
  if (props.index % 2 != 0) {
    color = ODD_ROW_COLOR
  }
  if (props.isChecked) {
    color = SELECTED_ROW_COLOR
  }
  return (
    <div
      key={props.tableKey}
      className={"job-row " + props.className}
      style={{
        ...props.style,
        background: color,
      }}
    >
      <div className="select-box">
        <Checkbox
          color={"secondary"}
          checked={props.isChecked}
          onClick={(event) => {
            if (event.shiftKey) {
              props.onChangeCheckedShift(!props.isChecked)
            } else {
              props.onChangeChecked(!props.isChecked)
            }
          }}
        />
      </div>
      {props.columns}
    </div>
  )
}
