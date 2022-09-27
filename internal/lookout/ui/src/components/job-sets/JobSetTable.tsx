import React from "react"

import Truncate from "react-truncate"
import { TableCellProps, Table as VirtualizedTable } from "react-virtualized"
import { Column, defaultTableCellRenderer } from "react-virtualized"

import { JobSet } from "../../services/JobService"
import CheckboxHeaderRow from "../CheckboxHeaderRow"
import CheckboxRow from "../CheckboxRow"
import LinkCell from "../LinkCell"
import SortableHeaderCell from "../SortableHeaderCell"

import "./JobSetTable.css"

interface JobSetTableProps {
  height: number
  width: number
  jobSets: JobSet[]
  selectedJobSets: Map<string, JobSet>
  newestFirst: boolean
  onJobSetClick: (jobSet: string, state: string) => void
  onSelectJobSet: (index: number, selected: boolean) => void
  onShiftSelectJobSet: (index: number, selected: boolean) => void
  onDeselectAllClick: () => void
  onSelectAllClick: () => void
  onOrderChange: (newestFirst: boolean) => void
}

function cellRendererForState(
  cellProps: TableCellProps,
  onJobSetClick: (jobSet: string, state: string) => void,
  state: string,
) {
  if (cellProps.cellData) {
    return <LinkCell onClick={() => onJobSetClick((cellProps.rowData as JobSet).jobSetId, state)} {...cellProps} />
  }
  return defaultTableCellRenderer(cellProps)
}

function cellRendererForJobSet(cellProps: TableCellProps, width: number) {
  return (
    <Truncate width={width * 1.5} lines={1}>
      {cellProps.cellData}
    </Truncate>
  )
}

export default function JobSetTable(props: JobSetTableProps) {
  return (
    <div
      style={{
        height: props.height,
        width: props.width,
      }}
    >
      <VirtualizedTable
        rowGetter={({ index }) => props.jobSets[index]}
        rowCount={props.jobSets.length}
        rowHeight={50}
        headerHeight={60}
        height={props.height}
        width={props.width}
        headerClassName="job-set-table-header"
        rowRenderer={(tableRowProps) => {
          return (
            <CheckboxRow
              isChecked={props.selectedJobSets.has(tableRowProps.rowData.jobSetId)}
              onChangeChecked={(selected) => props.onSelectJobSet(tableRowProps.index, selected)}
              onChangeCheckedShift={(selected) => props.onShiftSelectJobSet(tableRowProps.index, selected)}
              tableKey={tableRowProps.key}
              {...tableRowProps}
            />
          )
        }}
        headerRowRenderer={(tableHeaderRowProps) => {
          const jobSetsAreSelected = props.selectedJobSets.size > 0
          const noJobSetsArePresent = props.jobSets.length == 0
          return (
            <CheckboxHeaderRow
              checked={jobSetsAreSelected}
              disabled={!jobSetsAreSelected && noJobSetsArePresent}
              onClick={jobSetsAreSelected ? () => props.onDeselectAllClick() : props.onSelectAllClick}
              {...tableHeaderRowProps}
            />
          )
        }}
      >
        <Column
          dataKey="jobSetId"
          width={0.5 * props.width}
          label="Job Set"
          cellRenderer={(cellProps) => cellRendererForJobSet(cellProps, 0.5 * props.width)}
          className="job-set-table-job-set-name-cell"
        />
        <Column
          dataKey="latestSubmissionTime"
          width={0.14 * props.width}
          label="Submission Time"
          headerRenderer={(cellProps) => (
            <SortableHeaderCell
              name="Submission Time"
              descending={props.newestFirst}
              className="job-set-submission-time-header-cell"
              onOrderChange={props.onOrderChange}
              {...cellProps}
            />
          )}
        />
        <Column
          dataKey="jobsQueued"
          width={0.06 * props.width}
          label="Queued"
          className="job-set-table-number-cell"
          cellRenderer={(cellProps) => cellRendererForState(cellProps, props.onJobSetClick, "Queued")}
        />
        <Column
          dataKey="jobsPending"
          width={0.06 * props.width}
          label="Pending"
          className="job-set-table-number-cell"
          cellRenderer={(cellProps) => cellRendererForState(cellProps, props.onJobSetClick, "Pending")}
        />
        <Column
          dataKey="jobsRunning"
          width={0.06 * props.width}
          label="Running"
          className="job-set-table-number-cell"
          cellRenderer={(cellProps) => cellRendererForState(cellProps, props.onJobSetClick, "Running")}
        />
        <Column
          dataKey="jobsSucceeded"
          width={0.06 * props.width}
          label="Succeeded"
          className="job-set-table-number-cell"
          cellRenderer={(cellProps) => cellRendererForState(cellProps, props.onJobSetClick, "Succeeded")}
        />
        <Column
          dataKey="jobsFailed"
          width={0.06 * props.width}
          label="Failed"
          className="job-set-table-number-cell"
          cellRenderer={(cellProps) => cellRendererForState(cellProps, props.onJobSetClick, "Failed")}
        />
        <Column
          dataKey="jobsCancelled"
          width={0.06 * props.width}
          label="Cancelled"
          className="job-set-table-number-cell"
          cellRenderer={(cellProps) => cellRendererForState(cellProps, props.onJobSetClick, "Cancelled")}
        />
      </VirtualizedTable>
    </div>
  )
}
