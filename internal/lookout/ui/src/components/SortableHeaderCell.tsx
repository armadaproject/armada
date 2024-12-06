import { ArrowDropDown, ArrowDropUp } from "@mui/icons-material"
import { TableHeaderProps } from "react-virtualized"

import "./SortableHeaderCell.css"

type SortableHeaderCellProps = {
  descending: boolean
  name: string
  className: string
  onOrderChange: (descending: boolean) => void
} & TableHeaderProps

export default function SortableHeaderCell(props: SortableHeaderCellProps) {
  return (
    <div className={"sortable-header-cell " + props.className} onClick={() => props.onOrderChange(!props.descending)}>
      <div>{props.name}</div>
      <div>{props.descending ? <ArrowDropDown /> : <ArrowDropUp />}</div>
    </div>
  )
}
