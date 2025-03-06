import { Table, TableBody, TableCell, TableRow, Link, styled } from "@mui/material"
import validator from "validator"

import { ActionableValueOnHover } from "../../ActionableValueOnHover"

const StyledTableCell = styled(TableCell)({
  width: "50%",
  wordBreak: "break-all",
})

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
              <StyledTableCell>{key}</StyledTableCell>
              <StyledTableCell>
                <ActionableValueOnHover copyAction={allowCopy ? { copyContent: value } : undefined}>
                  {nodeToDisplay}
                </ActionableValueOnHover>
              </StyledTableCell>
            </TableRow>
          )
        })}
      </TableBody>
    </Table>
  )
}
