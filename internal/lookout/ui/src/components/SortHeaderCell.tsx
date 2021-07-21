import React from "react"

import { ArrowDropDown, ArrowDropUp } from "@material-ui/icons"
import { TableHeaderProps } from "react-virtualized"

import "./SortHeaderCell.css"

type SortHeaderCellProps = {
  title: string
  desc: boolean
  onOrderChange: (desc: boolean) => void
} & TableHeaderProps

export default function SortHeaderCell(props: SortHeaderCellProps) {
  return (
    <div className="sort-header-cell" onClick={() => props.onOrderChange(!props.desc)}>
      <div className="sort-header-cell-label">{props.title}</div>
      <div className="sort-header-cell-icon">{props.desc ? <ArrowDropDown /> : <ArrowDropUp />}</div>
    </div>
  )
}
