import React from "react"

import { Column } from "react-virtualized"

import { ColumnSpec } from "../../containers/JobsContainer"
import { Job } from "../../services/JobService"
import LinkCell from "../LinkCell"
import JobStateCell from "./JobStateCell"
import JobStatesHeaderCell from "./JobStatesHeaderCell"
import SearchHeaderCell from "./SearchHeaderCell"
import SubmissionTimeHeaderCell from "./SubmissionTimeHeaderCell"

import "./JobTableColumns.css"

const SubmissionTimeColumn = (props: {
  key: string | number
  dataKey: string
  width: number
  label: string
  ordering: boolean
  onOrderChange: (descending: boolean) => void
}) => (
  <Column
    key={props.key}
    dataKey={props.dataKey}
    width={props.width}
    label={props.label}
    headerRenderer={(headerProps) => (
      <SubmissionTimeHeaderCell newestFirst={props.ordering} onOrderChange={props.onOrderChange} {...headerProps} />
    )}
  />
)

const JobStateColumn = (props: {
  key: string | number
  dataKey: string
  width: number
  label: string
  jobStates: string[]
  onJobStateChange: (jobStates: string[]) => void
}) => (
  <Column
    key={props.key}
    dataKey={props.dataKey}
    width={props.width}
    label={props.label}
    cellRenderer={(cellProps) => <JobStateCell {...cellProps} />}
    style={{ height: "100%" }}
    headerRenderer={(headerProps) => (
      <JobStatesHeaderCell
        jobStates={props.jobStates as string[]}
        onJobStatesChange={props.onJobStateChange}
        {...headerProps}
      />
    )}
  />
)

const JobIdColumn = (props: {
  key: string | number
  dataKey: string
  width: number
  label: string
  jobId: string
  onJobIdChange: (jobId: string) => void
  onJobClick: (jobIndex: number) => void
}) => (
  <Column
    key={props.key}
    dataKey={props.dataKey}
    width={props.width}
    label={props.label}
    cellRenderer={(cellProps) => <LinkCell onClick={() => props.onJobClick(cellProps.rowIndex)} {...cellProps} />}
    headerRenderer={(headerProps) => (
      <SearchHeaderCell headerLabel={props.label} value={props.jobId} onChange={props.onJobIdChange} {...headerProps} />
    )}
  />
)

const StringValueColumn = (props: {
  key: string | number
  dataKey: string
  width: number
  label: string
  value: string
  onChange: (val: string) => void
}) => (
  <Column
    key={props.key}
    dataKey={props.dataKey}
    width={props.width}
    label={props.label}
    headerRenderer={(headerProps) => (
      <SearchHeaderCell headerLabel={props.label} value={props.value} onChange={props.onChange} {...headerProps} />
    )}
  />
)

const AnnotationColumn = (props: {
  key: string | number
  dataKey: string
  width: number
  label: string
  value: string
  onChange: (val: string) => void
}) => (
  <Column
    key={props.key}
    dataKey={props.dataKey}
    cellDataGetter={({ dataKey, rowData }) => {
      const job = rowData as Job
      if (job.annotations[dataKey]) {
        return job.annotations[dataKey]
      }
    }}
    width={props.width}
    label={props.label}
    headerRenderer={(headerProps) => (
      <SearchHeaderCell headerLabel={props.label} value={props.value} onChange={props.onChange} {...headerProps} />
    )}
  />
)

function calculateFillWidth(columns: ColumnSpec<any>[], totalWidth: number) {
  let leftoverWidth = totalWidth
  let fillColumns = 0
  for (const col of columns) {
    if (col.width === "Fill") {
      fillColumns++
    } else {
      leftoverWidth -= col.width
    }
  }
  return Math.floor(leftoverWidth / fillColumns)
}

type JobTableColumnsProps = {
  columns: ColumnSpec<string | boolean | string[]>[]
  totalWidth: number
  onChangeColumnValue: (id: string, val: string | boolean | string[]) => void
  onJobClick: (jobIndex: number) => void
}

export default function JobTableColumns(props: JobTableColumnsProps) {
  const fillWidth = calculateFillWidth(props.columns, props.totalWidth)
  return (
    <>
      {props.columns.map((col, i) => {
        switch (col.id) {
          case "submissionTime": {
            return (
              <SubmissionTimeColumn
                key={i}
                dataKey={col.accessor}
                width={col.width === "Fill" ? fillWidth : col.width}
                label={col.name}
                ordering={col.filter as boolean}
                onOrderChange={(newValue) => props.onChangeColumnValue(col.id, newValue)}
              />
            )
          }
          case "jobState": {
            return (
              <JobStateColumn
                key={i}
                dataKey={col.accessor}
                width={col.width === "Fill" ? fillWidth : col.width}
                label={col.name}
                jobStates={col.filter as string[]}
                onJobStateChange={(newValue) => props.onChangeColumnValue(col.id, newValue)}
              />
            )
          }
          case "jobId": {
            return (
              <JobIdColumn
                key={i}
                dataKey={col.accessor}
                width={col.width === "Fill" ? fillWidth : col.width}
                label={col.name}
                jobId={col.filter as string}
                onJobIdChange={(newValue) => props.onChangeColumnValue(col.id, newValue)}
                onJobClick={props.onJobClick}
              />
            )
          }
          case "queue":
          case "jobSet":
          case "owner": {
            return (
              <StringValueColumn
                key={i}
                dataKey={col.accessor}
                width={col.width === "Fill" ? fillWidth : col.width}
                label={col.name}
                value={col.name}
                onChange={(newValue) => props.onChangeColumnValue(col.id, newValue)}
              />
            )
          }
          default: {
            return (
              <AnnotationColumn
                key={i}
                dataKey={col.accessor}
                width={col.width === "Fill" ? fillWidth : col.width}
                label={col.name}
                value={col.name}
                onChange={(newValue) => props.onChangeColumnValue(col.id, newValue)}
              />
            )
          }
        }
      })}
    </>
  )
}
