import React, { useState } from "react"

import { Checkbox } from "@material-ui/core"
import { TableRowProps } from "react-virtualized"

import "./Row.css"

type CheckboxRowProps = {
  isChecked: boolean
  onChangeChecked: (checked: boolean) => void
  onChangeCheckedShift: (checked: boolean) => void
  tableKey: string
} & TableRowProps

export const CHECKBOX_WIDTH = 70

const EVEN_ROW_COLOR = "#ffffff"
const ODD_ROW_COLOR = "#f5f5f5"
const SELECTED_ROW_COLOR = "#b2ebf2"
const HOVERED_ROW_COLOR = "#e0e0e0"

export default function CheckboxRow(props: CheckboxRowProps) {
  const [isHovered, setHovered] = useState(false)

  let color = EVEN_ROW_COLOR
  if (props.index % 2 != 0) {
    color = ODD_ROW_COLOR
  }
  if (isHovered) {
    color = HOVERED_ROW_COLOR
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
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
    >
      <div
        className="select-box"
        style={{
          width: CHECKBOX_WIDTH,
        }}
      >
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
