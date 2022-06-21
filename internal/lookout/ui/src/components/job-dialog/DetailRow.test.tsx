import React from "react"

import { TableContainer, Table, TableBody } from "@material-ui/core"
import { render, screen } from "@testing-library/react"

import DetailRow from "./DetailRow"
function SetUpReactTable(name: string, value: string, isAnnotation?: boolean) {
  return (
    <TableContainer>
      <Table className="details-table-container">
        <TableBody>
          <DetailRow key={name} isAnnotation={isAnnotation} detailRowKey={name} name={name} value={value} />
        </TableBody>
      </Table>
    </TableContainer>
  )
}
describe("DetailRow", () => {
  it("DetailRow with no links in annotations", async () => {
    const actual = SetUpReactTable("detail", "NOTURL", true)
    render(actual)
    const noLink = await screen.queryByRole("link")
    expect(noLink).toBeFalsy()
    expect(screen.getByText("NOTURL")).toBeInTheDocument()
  })
  it("DetailRow with links in annotations", async () => {
    const actual = SetUpReactTable("detail", "http://google.org", true)
    render(actual)
    const linkRole = await screen.queryByRole("link")
    expect(linkRole).toBeTruthy()
    expect(screen.getByText("http://google.org")).toBeInTheDocument()
  })
  it("DetailRow With Bad Link", async () => {
    const actual = SetUpReactTable("detail", "//google.org")
    render(actual)
    const linkRole = await screen.queryByRole("//google.org")
    expect(linkRole).toBeFalsy()
    expect(screen.getByText("//google.org")).toBeInTheDocument()
  })
  it("DetailRow With Bad Link in annotation", async () => {
    const actual = SetUpReactTable("detail", "//google.org", true)
    render(actual)
    const linkRole = await screen.queryByRole("//google.org")
    expect(linkRole).toBeFalsy()
    expect(screen.getByText("//google.org")).toBeInTheDocument()
  })

  it("DetailRow With Link But No Annotation", async () => {
    const actual = SetUpReactTable("detail", "http://google.org")
    render(actual)
    const linkRole = await screen.queryByRole("http://google.org")
    expect(linkRole).toBeFalsy()
    expect(screen.getByText("http://google.org")).toBeInTheDocument()
  })
})
