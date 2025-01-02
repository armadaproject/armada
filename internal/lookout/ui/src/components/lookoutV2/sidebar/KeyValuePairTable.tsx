import { Table, TableBody, TableCell, TableRow, Link } from "@mui/material"
import validator from "validator"

import styles from "./KeyValuePairTable.module.css"
import { ActionableValueOnHover } from "../../ActionableValueOnHover"

export interface KeyValuePairTable {
  data: {
    key: string
    value: string
    isAnnotation?: boolean
    allowCopy?: boolean
  }[]
}

const ensureAbsoluteLink = (link: string): string => {
  if (link.startsWith("//") || link.startsWith("http")) {
    return link
  }
  return "//" + link
}

export const KeyValuePairTable = ({ data }: KeyValuePairTable) => {
  return (
    <Table size="small">
      <TableBody>
        {data.map(({ key, value, isAnnotation, allowCopy }) => {
          const nodeToDisplay =
            isAnnotation && validator.isURL(value) ? (
              <Link href={ensureAbsoluteLink(value)} target="_blank">
                {value}
              </Link>
            ) : (
              <span>{value}</span>
            )
          return (
            <TableRow key={key}>
              <TableCell className={styles.cell}>{key}</TableCell>
              <TableCell className={styles.cell}>
                <ActionableValueOnHover copyAction={allowCopy ? { copyContent: value } : undefined}>
                  {nodeToDisplay}
                </ActionableValueOnHover>
              </TableCell>
            </TableRow>
          )
        })}
      </TableBody>
    </Table>
  )
}
