import React from "react"

import { TableContainer, Table, TableBody } from "@material-ui/core"
import { render, screen } from "@testing-library/react"

import RunDetailsRows from "./RunDetailsRows"
import { Run } from "../../services/JobService"

function SetUpReactTable(run: Run, jobId: string) {
  return (
    <TableContainer>
      <Table className="details-table-container">
        <TableBody>
          <RunDetailsRows run={run} jobId={jobId} />
        </TableBody>
      </Table>
    </TableContainer>
  )
}

describe("RunDetailsRow", () => {
  it("Defaults", () => {
    const run = { succeeded: true, cluster: "test", podNumber: 0, k8sId: "test" }
    render(SetUpReactTable(run, "jobId"))
    expect(screen.getByText("armada-jobId-0")).toBeInTheDocument()
  })
  it("All Parameters", () => {
    const run = {
      node: "demo",
      podCreationTime: "42",
      podStartTime: "42",
      finishTime: "42",
      error: "NoErrorInTest",
      succeeded: true,
      cluster: "test",
      podNumber: 0,
      k8sId: "test",
    }
    render(SetUpReactTable(run, "jobId"))
    expect(screen.getByText("armada-jobId-0")).toBeInTheDocument()
    expect(screen.getByText("NoErrorInTest")).toBeInTheDocument()
  })
})
