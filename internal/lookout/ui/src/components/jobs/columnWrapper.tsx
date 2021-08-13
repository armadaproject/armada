import React from "react"

import { grey, green, orange, purple, red, yellow } from "@material-ui/core/colors"
import { Column, TableCellProps } from "react-virtualized"

import { ColumnSpec } from "../../containers/JobsContainer"
import { Job } from "../../services/JobService"
import LinkCell from "../LinkCell"
import SortableHeaderCell from "../SortableHeaderCell"
import JobStatesHeaderCell from "./JobStatesHeaderCell"
import SearchHeaderCell from "./SearchHeaderCell"

export default function columnWrapper(
  key: string,
  columnSpec: ColumnSpec<string | boolean | string[]>,
  width: number,
  onChange: (val: string | boolean | string[]) => void,
  onJobIdClick: (jobIndex: number) => void,
) {
  let column

  switch (columnSpec.id) {
    case "submissionTime": {
      column = (
        <Column
          key={key}
          dataKey={columnSpec.accessor}
          width={width}
          label={columnSpec.name}
          headerRenderer={(headerProps) => (
            <SortableHeaderCell
              name="Submission Time"
              descending={columnSpec.filter as boolean}
              className="job-submission-time-header-cell"
              onOrderChange={onChange}
              {...headerProps}
            />
          )}
        />
      )
      break
    }
    case "jobState": {
      column = (
        <Column
          key={key}
          dataKey={columnSpec.accessor}
          width={100}
          label={columnSpec.name}
          cellRenderer={(cellProps) => cellRendererForState(cellProps, cellProps.cellData)}
          style={{ height: "100%" }}
          headerRenderer={(headerProps) => (
            <JobStatesHeaderCell
              jobStates={columnSpec.filter as string[]}
              onJobStatesChange={onChange}
              {...headerProps}
            />
          )}
        />
      )
      break
    }
    case "jobId": {
      column = (
        <Column
          key={key}
          dataKey={columnSpec.accessor}
          width={width}
          label={columnSpec.name}
          cellRenderer={(cellProps) => <LinkCell onClick={() => onJobIdClick(cellProps.rowIndex)} {...cellProps} />}
          headerRenderer={(headerProps) => (
            <SearchHeaderCell
              headerLabel={columnSpec.name}
              value={columnSpec.filter as string}
              onChange={onChange}
              {...headerProps}
            />
          )}
        />
      )
      break
    }
    case "queue":
    case "jobSet":
    case "owner": {
      column = (
        <Column
          key={key}
          dataKey={columnSpec.accessor}
          width={width}
          label={columnSpec.name}
          headerRenderer={(headerProps) => (
            <SearchHeaderCell
              headerLabel={columnSpec.name}
              value={columnSpec.filter as string}
              onChange={onChange}
              {...headerProps}
            />
          )}
        />
      )
      break
    }
    default: {
      column = (
        <Column
          key={key}
          dataKey={columnSpec.accessor}
          cellDataGetter={({ dataKey, rowData }) => {
            const job = rowData as Job
            if (job.annotations[dataKey]) {
              return job.annotations[dataKey]
            }
          }}
          width={width}
          label={columnSpec.name}
          headerRenderer={(headerProps) => (
            <SearchHeaderCell
              headerLabel={columnSpec.name}
              value={columnSpec.filter as string}
              onChange={onChange}
              {...headerProps}
            />
          )}
        />
      )
      break
    }
  }

  return column
}

function cellRendererForState(cellProps: TableCellProps, state: string) {
  return (
    <div
      style={{
        backgroundColor: colorForState(state),
        display: "flex",
        flex: 1,
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
        height: "100%",
      }}
    >
      {state}
    </div>
  )
}

function colorForState(state: string): string {
  switch (state) {
    case "Queued":
      return yellow["A100"]
    case "Pending":
      return orange["A100"]
    case "Running":
      return green["A100"]
    case "Succeeded":
      return "white"
    case "Failed":
      return red["A100"]
    case "Cancelled":
      return grey[300]
    default:
      return purple["A100"]
  }
}
