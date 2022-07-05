import React from "react"

import { Checkbox } from "@material-ui/core"
import { TableHeaderRowProps } from "react-virtualized"

import { CHECKBOX_WIDTH } from "./CheckboxRow"

import "./Row.css"

export type CheckboxHeaderRowProps = {
  checked: boolean
  disabled: boolean
  onClick: () => void
} & TableHeaderRowProps

export default function CheckboxHeaderRow(props: CheckboxHeaderRowProps) {
  return (
    <div className={props.className} style={props.style}>
      <div className="select-box" style={{ width: CHECKBOX_WIDTH }}>
        <Checkbox
          checked={props.checked}
          disabled={props.disabled}
          indeterminate={props.checked}
          onClick={props.onClick}
        />
      </div>
      {props.columns}
    </div>
  )
}
