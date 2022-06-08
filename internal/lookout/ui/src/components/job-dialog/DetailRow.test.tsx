import React from "react"

import { TableContainer, Table, TableBody } from "@material-ui/core"
import { render, screen } from "@testing-library/react"

import DetailRow from "./DetailRow"
function SetUpReactTable(name: string, value: string) {
  return (
    <TableContainer>
      <Table className="details-table-container">
        <TableBody>
          <DetailRow key={"annotation-" + name} name={name} value={value} />
        </TableBody>
      </Table>
    </TableContainer>
  )
}
describe("DetailRow", () => {
  it("DetailRow with no links", () => {
    const actual = SetUpReactTable("detail", "NOTURL")
    render(actual)
    expect(screen.getByText("NOTURL")).toBeInTheDocument()
  })
  it("DetailRow with links", () => {
    const actual = SetUpReactTable("detail", "http://google.org")
    render(actual)
    expect(screen.getByRole("link")).toBeInTheDocument()
  })
  it("DetailRow With Bad Link", () => {
    const actual = SetUpReactTable("detail", "//google.org")
    render(actual)
    expect(screen.getByText("//google.org")).toBeInTheDocument()
  })
})
