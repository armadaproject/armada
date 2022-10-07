import React from "react"

import { TextField } from "@material-ui/core"
import Draggable from "react-draggable"
import { TableHeaderProps } from "react-virtualized"

import "./SearchHeaderCell.css"

type SearchHeaderCellProps = {
  headerLabel: string
  value: string
  onChange: (newValue: string) => void
  onResizeColumns: (data: number) => void
} & TableHeaderProps

export default function SearchHeaderCell(props: SearchHeaderCellProps) {
  return (
    <Draggable
      axis="x"
      defaultClassName="DragHandle"
      defaultClassNameDragging="DragHandleActive"
      onStop={(event, data) => props.onResizeColumns(data.x)}
      position={{ x: 0, y: 0 }}
    >
      <div className="search-header">
        <TextField
          InputProps={{
            className: "search-header-text-field-input",
          }}
          label={props.headerLabel ? props.headerLabel : "Blank column"}
          value={props.value}
          disabled={props.headerLabel === ""}
          onChange={(event) => props.onChange(event.target.value)}
          className="search-header-text-field"
        />
      </div>
    </Draggable>
  )
}
