import { Table, TableBody, TableCell, TableRow, Link } from "@mui/material"
import validator from "validator"

import styles from "./KeyValuePairTable.module.css"

export interface KeyValuePairTable {
  data: {
    key: string
    value: string
    isAnnotation?: boolean
  }[]
}

const ensureAbsoluteLink = (link: string): string => {
  if (link.startsWith("//")) {
    return link
  }
  return "//" + link
}

export const KeyValuePairTable = ({ data }: KeyValuePairTable) => {
  return (
    <Table size="small">
      <TableBody>
        {data.map(({ key, value, isAnnotation }) => {
          return (
            <TableRow key={key}>
              <TableCell className={styles.cell}>{key}</TableCell>
              <TableCell className={styles.cell}>
                {isAnnotation && validator.isURL(value) ? (
                  <Link href={ensureAbsoluteLink(value)} target="_blank">
                    {value}
                  </Link>
                ) : (
                  <span>{value}</span>
                )}
              </TableCell>
            </TableRow>
          )
        })}
      </TableBody>
    </Table>
  )
}
