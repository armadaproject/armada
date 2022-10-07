import React from "react"

import { ArrowDropDown, ArrowDropUp } from "@material-ui/icons"
import Draggable from "react-draggable"
import { TableHeaderProps } from "react-virtualized"

import "./SortableHeaderCell.css"
import "./job-sets/JobSetTable.css"

type SortableHeaderCellProps = {
  descending: boolean
  name: string
  className: string
  onOrderChange: (descending: boolean) => void
  onResizeColumns: (deltaX: number) => void
} & TableHeaderProps

export default function SortableHeaderCell(props: SortableHeaderCellProps) {
  return (
    <Draggable
      axis="x"
      defaultClassName="DragHandle"
      defaultClassNameDragging="DragHandleActive"
      onStop={(event, data) => props.onResizeColumns(data.x)}
      position={{ x: 0, y: 0 }}
    >
      <div className={"sortable-header-cell " + props.className} onClick={() => props.onOrderChange(!props.descending)}>
        <div>{props.name}</div>
        <div>{props.descending ? <ArrowDropDown /> : <ArrowDropUp />}</div>
      </div>
    </Draggable>
  )
}
