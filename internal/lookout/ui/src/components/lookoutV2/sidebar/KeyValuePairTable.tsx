import { Table, TableBody, TableCell, TableRow } from "@mui/material"

import styles from "./KeyValuePairTable.module.css"

export interface KeyValuePairTable {
  data: {
    key: string
    value: string
  }[]
}
export const KeyValuePairTable = ({ data }: KeyValuePairTable) => {
  return (
    <Table sx={{ marginBottom: "1.5em" }} size="small">
      <TableBody>
        {data.map(({ key, value }) => {
          return (
            <TableRow key={key}>
              <TableCell className={styles.cell}>{key}</TableCell>
              <TableCell className={styles.cell}>{value}</TableCell>
            </TableRow>
          )
        })}
      </TableBody>
    </Table>
  )
}
