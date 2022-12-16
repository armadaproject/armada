import { Table, TableBody, TableCell, TableRow } from "@mui/material"

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
              <TableCell sx={{ width: "50%" }}>{key}</TableCell>
              <TableCell sx={{ width: "50%" }}>{value}</TableCell>
            </TableRow>
          )
        })}
      </TableBody>
    </Table>
  )
}
