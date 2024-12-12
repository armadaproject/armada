import Truncate from "react-truncate"
import { TableCellProps, Table as VirtualizedTable, Column } from "react-virtualized"

import { JobState, jobStateColors, jobStateIcons } from "../../models/lookoutV2Models"
import { JobSet } from "../../services/JobService"
import CheckboxHeaderRow from "../CheckboxHeaderRow"
import CheckboxRow from "../CheckboxRow"
import "./JobSetTable.css"
import SortableHeaderCell from "../SortableHeaderCell"
import { JobStateCountChip } from "../lookoutV2/JobStateCountChip"
import { Stack } from "@mui/material"
import { formatJobState } from "../../utils/jobsTableFormatters"

interface JobSetTableProps {
  height: number
  width: number
  queue: string
  jobSets: JobSet[]
  selectedJobSets: Map<string, JobSet>
  newestFirst: boolean
  onSelectJobSet: (index: number, selected: boolean) => void
  onShiftSelectJobSet: (index: number, selected: boolean) => void
  onDeselectAllClick: () => void
  onSelectAllClick: () => void
  onOrderChange: (newestFirst: boolean) => void
  onJobSetStateClick(rowIndex: number, state: string): void
}

function cellRendererForState(cellProps: TableCellProps, jobState: JobState, onClickFunc: () => void) {
  return <JobStateCountChip state={jobState} count={cellProps.cellData} onClick={onClickFunc} />
}

function cellRendererForJobSet(cellProps: TableCellProps, width: number) {
  return (
    <Truncate width={width * 1.5} lines={1}>
      {cellProps.cellData}
    </Truncate>
  )
}

export default function JobSetTable(props: JobSetTableProps) {
  if (props.queue === "") {
    return (
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          height: props.height,
          width: props.width,
        }}
      >
        Enter a queue name into the "Queue" field to view job sets.
      </div>
    )
  }
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
          headerRenderer={(cellProps) => cellProps.label}
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
        {(
          [
            [JobState.Queued, "jobsQueued"],
            [JobState.Pending, "jobsPending"],
            [JobState.Running, "jobsRunning"],
            [JobState.Succeeded, "jobsSucceeded"],
            [JobState.Failed, "jobsFailed"],
            [JobState.Cancelled, "jobsCancelled"],
          ] as [JobState, keyof JobSet][]
        ).map(([jobState, dataKey]) => {
          const Icon = jobStateIcons[jobState]
          return (
            <Column
              key={jobState}
              dataKey={dataKey}
              width={0.06 * props.width}
              label={
                <Stack direction="row" spacing={1} alignItems="center">
                  <span>{formatJobState(jobState)}</span>
                  <Icon fontSize="inherit" color={jobStateColors[jobState]} />
                </Stack>
              }
              className="job-set-table-number-cell"
              cellRenderer={(cellProps) =>
                cellRendererForState(cellProps, jobState, () => props.onJobSetStateClick(cellProps.rowIndex, jobState))
              }
            />
          )
        })}
      </VirtualizedTable>
    </div>
  )
}
