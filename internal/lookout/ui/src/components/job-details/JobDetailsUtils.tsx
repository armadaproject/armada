import React from "react"

import { Button, TableCell, TableRow } from "@material-ui/core"
import FileCopyOutlined from "@material-ui/icons/FileCopyOutlined"

export function MakeJobDetailsRow(name: string, value: string) {
  return MakeJobDetailSpecifyKey(name, name, value)
}

export function MakeJobDetailSpecifyKey(key: string, name: string, value: string) {
  return MakeJobDetailsRowDetailed(key, name, value, "field-value")
}

export function MakeJobDetailsRowDetailed(key: string, name: string, value: string, valueClass: string) {
  return (
    <TableRow key={key}>
      <TableCell className="field-label">{name}</TableCell>
      <TableCell className={valueClass}>{value}</TableCell>
      <TableCell className="copy">
        <Button
          onClick={() => {
            navigator.clipboard.writeText(value)
          }}
          startIcon={<FileCopyOutlined />}
        />
      </TableCell>
    </TableRow>
  )
}
