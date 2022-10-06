import React from "react"

import Draggable from "react-draggable"
import Truncate from "react-truncate"
import { TableCellProps, Table as VirtualizedTable, TableHeaderProps } from "react-virtualized"
import { Column, defaultTableCellRenderer } from "react-virtualized"

import { JobSetWidths } from "../../containers/JobSetsContainer"
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
  jobSetWidths: JobSetWidths
  selectedJobSets: Map<string, JobSet>
  newestFirst: boolean
  onJobSetClick: (jobSet: string, state: string) => void
  onSelectJobSet: (index: number, selected: boolean) => void
  onShiftSelectJobSet: (index: number, selected: boolean) => void
  onDeselectAllClick: () => void
  onSelectAllClick: () => void
  onOrderChange: (newestFirst: boolean) => void
  onResizeColumns: (dataKey: keyof JobSetWidths, deltaX: number) => void
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

function headerRender(props: TableHeaderProps, onResizeColumns: (dataKey: keyof JobSetWidths, deltaX: number) => void) {
  return (
    <React.Fragment key={props.dataKey}>
      <div>{props.label}</div>
      <Draggable
        axis="x"
        defaultClassName="DragHandle"
        defaultClassNameDragging="DragHandleActive"
        onStop={(event, data) => onResizeColumns(props.dataKey as keyof JobSetWidths, data.x)}
        position={{ x: 0, y: 0 }}
      >
        <span className="DragHandleIcon">⋮</span>
      </Draggable>
    </React.Fragment>
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
          width={props.jobSetWidths.jobSetId * props.width}
          label="Job Set"
          cellRenderer={(cellProps) => cellRendererForJobSet(cellProps, props.jobSetWidths.jobSetId * props.width)}
          className="job-set-table-job-set-name-cell"
          headerRenderer={(headerProps) => headerRender(headerProps, props.onResizeColumns)}
        />
        <Column
          dataKey="latestSubmissionTime"
          width={props.jobSetWidths.latestSubmissionTime * props.width}
          label="Submission Time"
          headerRenderer={(cellProps) => (
            <div>
              <SortableHeaderCell
                name="Submission Time"
                descending={props.newestFirst}
                className="job-set-submission-time-header-cell"
                onOrderChange={props.onOrderChange}
                {...cellProps}
              />
              {headerRender(cellProps, props.onResizeColumns)}
            </div>
          )}
        />
        <Column
          dataKey="jobsQueued"
          width={props.jobSetWidths.jobsQueued * props.width}
          label="Queued"
          className="job-set-table-number-cell"
          cellRenderer={(cellProps) => cellRendererForState(cellProps, props.onJobSetClick, "Queued")}
          headerRenderer={(headerProps) => headerRender(headerProps, props.onResizeColumns)}
        />
        <Column
          dataKey="jobsPending"
          width={props.jobSetWidths.jobsPending * props.width}
          label="Pending"
          className="job-set-table-number-cell"
          cellRenderer={(cellProps) => cellRendererForState(cellProps, props.onJobSetClick, "Pending")}
          headerRenderer={(headerProps) => headerRender(headerProps, props.onResizeColumns)}
        />
        <Column
          dataKey="jobsRunning"
          width={props.jobSetWidths.jobsRunning * props.width}
          label="Running"
          className="job-set-table-number-cell"
          cellRenderer={(cellProps) => cellRendererForState(cellProps, props.onJobSetClick, "Running")}
          headerRenderer={(headerProps) => headerRender(headerProps, props.onResizeColumns)}
        />
        <Column
          dataKey="jobsSucceeded"
          width={props.jobSetWidths.jobsSucceeded * props.width}
          label="Succeeded"
          className="job-set-table-number-cell"
          cellRenderer={(cellProps) => cellRendererForState(cellProps, props.onJobSetClick, "Succeeded")}
          headerRenderer={(headerProps) => headerRender(headerProps, props.onResizeColumns)}
        />
        <Column
          dataKey="jobsFailed"
          width={props.jobSetWidths.jobsFailed * props.width}
          label="Failed"
          className="job-set-table-number-cell"
          cellRenderer={(cellProps) => cellRendererForState(cellProps, props.onJobSetClick, "Failed")}
          headerRenderer={(headerProps) => headerRender(headerProps, props.onResizeColumns)}
        />
        <Column
          dataKey="jobsCancelled"
          width={props.jobSetWidths.jobsCancelled * props.width}
          label="Cancelled"
          className="job-set-table-number-cell"
          cellRenderer={(cellProps) => cellRendererForState(cellProps, props.onJobSetClick, "Cancelled")}
          headerRenderer={(headerProps) => headerRender(headerProps, props.onResizeColumns)}
        />
      </VirtualizedTable>
    </div>
  )
}
