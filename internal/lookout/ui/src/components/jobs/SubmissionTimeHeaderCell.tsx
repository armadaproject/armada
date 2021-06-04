import React from "react"

import { ArrowDropDown, ArrowDropUp } from "@material-ui/icons"
import { TableHeaderProps } from "react-virtualized"

import "./SubmissionTimeHeaderCell.css"

type SubmissionTimeHeaderCellProps = {
  newestFirst: boolean
  onOrderChange: (newestFirst: boolean) => void
} & TableHeaderProps

export default function SubmissionTimeHeaderCell(props: SubmissionTimeHeaderCellProps) {
  return (
    <div className="submission-time-header-cell" onClick={() => props.onOrderChange(!props.newestFirst)}>
      <div className="submission-time-header-cell-label">Submission Time</div>
      <div className="submission-time-header-cell-icon">{props.newestFirst ? <ArrowDropDown /> : <ArrowDropUp />}</div>
    </div>
  )
}
