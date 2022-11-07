import React from "react"

import { Column } from "react-virtualized"

import { ColumnSpec } from "../../containers/JobsContainer"
import { Job } from "../../services/JobService"
import DefaultHeaderCell from "../DefaultHeaderCell"
import LinkCell from "../LinkCell"
import SortableHeaderCell from "../SortableHeaderCell"
import JobStateCell from "./JobStateCell"
import JobStatesHeaderCell from "./JobStatesHeaderCell"
import SearchHeaderCell from "./SearchHeaderCell"

import "./JobTableColumns.css"

type JobTableColumnsProps = {
  columns: ColumnSpec<string | boolean | string[]>[]
  onChangeColumnValue: (id: string, val: string | boolean | string[]) => void
  onJobIdClick: (jobIndex: number) => void
}

// Cannot be a custom component, react-virtualized requires a list of <Column>
export default function createJobTableColumns(props: JobTableColumnsProps) {
  return props.columns.map((col, i) => {
    switch (col.id) {
      case "submissionTime": {
        return (
          <Column
            key={i}
            dataKey={col.accessor}
            width={col.width}
            label={col.name}
            headerRenderer={(headerProps) => (
              <SortableHeaderCell
                name="Submission Time"
                className="job-submission-time-header-cell"
                descending={col.filter as boolean}
                onOrderChange={(newValue) => props.onChangeColumnValue(col.id, newValue)}
                {...headerProps}
              />
            )}
          />
        )
      }
      case "jobState": {
        return (
          <Column
            key={i}
            dataKey={col.accessor}
            width={col.width}
            label={col.name}
            cellRenderer={(cellProps) => <JobStateCell {...cellProps} />}
            style={{ height: "100%" }}
            headerRenderer={(headerProps) => (
              <JobStatesHeaderCell
                jobStates={col.filter as string[]}
                onJobStatesChange={(newValue) => props.onChangeColumnValue(col.id, newValue)}
                {...headerProps}
              />
            )}
          />
        )
      }
      case "jobId": {
        return (
          <Column
            key={i}
            dataKey={col.accessor}
            width={col.width}
            label={col.name}
            cellRenderer={(cellProps) => (
              <LinkCell onClick={() => props.onJobIdClick(cellProps.rowIndex)} {...cellProps} />
            )}
            headerRenderer={(headerProps) => (
              <SearchHeaderCell
                headerLabel={col.name}
                value={col.filter as string}
                onChange={(newValue) => props.onChangeColumnValue(col.id, newValue)}
                {...headerProps}
              />
            )}
          />
        )
      }
      case "jobStateDuration": {
        return (
          <Column
            key={i}
            dataKey={col.accessor}
            width={col.width}
            label={col.name}
            headerRenderer={(headerProps) => (
              <DefaultHeaderCell name={col.name} className="default-header-cell" {...headerProps} />
            )}
          />
        )
      }
      case "queue":
      case "jobSet":
      case "owner": {
        return (
          <Column
            key={i}
            dataKey={col.accessor}
            width={col.width}
            label={col.name}
            headerRenderer={(headerProps) => (
              <SearchHeaderCell
                headerLabel={col.name}
                value={col.filter as string}
                onChange={(newValue) => props.onChangeColumnValue(col.id, newValue)}
                {...headerProps}
              />
            )}
          />
        )
      }
      default: {
        return (
          <Column
            key={i}
            dataKey={col.accessor}
            cellDataGetter={({ dataKey, rowData }) => {
              const job = rowData as Job
              if (job.annotations[dataKey]) {
                return job.annotations[dataKey]
              }
            }}
            width={col.width}
            label={col.name}
            headerRenderer={(headerProps) => (
              <SearchHeaderCell
                headerLabel={col.name}
                value={col.filter as string}
                onChange={(newValue) => props.onChangeColumnValue(col.id, newValue)}
                {...headerProps}
              />
            )}
          />
        )
      }
    }
  })
}
