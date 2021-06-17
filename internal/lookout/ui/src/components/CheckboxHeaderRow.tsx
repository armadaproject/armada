import React from "react"

import { Checkbox } from "@material-ui/core"
import { TableHeaderRowProps } from "react-virtualized"

import "./Row.css"

export type CheckboxHeaderRowProps = {
  deselectEnabled: boolean
  onDeselectAllClick: () => void
} & TableHeaderRowProps

export default function CheckboxHeaderRow(props: CheckboxHeaderRowProps) {
  return (
    <div className={props.className} style={props.style}>
      <div className="select-box">
        <Checkbox
          checked={props.deselectEnabled}
          disabled={!props.deselectEnabled}
          indeterminate={props.deselectEnabled}
          onClick={props.onDeselectAllClick}
        />
      </div>
      {props.columns}
    </div>
  )
}
