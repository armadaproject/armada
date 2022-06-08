import React from "react"

import { IconButton, TableCell, TableRow, Link } from "@material-ui/core"
import FileCopyOutlined from "@material-ui/icons/FileCopyOutlined"
import validator from "validator"
type DetailRowProps = {
  detailRowKey?: string
  name: string
  value: string
  className?: string // Class to be applied to value <span> element
}
export default function DetailRow(props: DetailRowProps) {
  const key = props.detailRowKey ? props.detailRowKey : props.name
  return (
    <TableRow key={key}>
      <TableCell className="field-label">{props.name}</TableCell>
      <TableCell className="field-value">
        {validator.isURL(props.value) ? (
          <Link href={props.value}>{props.value}</Link>
        ) : (
          <span className={props.className || ""}>{props.value}</span>
        )}
      </TableCell>
      <TableCell className="copy" align="center">
        <IconButton
          onClick={() => {
            navigator.clipboard.writeText(props.value)
          }}
        >
          <FileCopyOutlined />
        </IconButton>
      </TableCell>
    </TableRow>
  )
}
