import React from "react"
import { Button, IconButton } from "@material-ui/core"
import RefreshIcon from '@material-ui/icons/Refresh'
import CancelIcon from "@material-ui/icons/Cancel"

import './JobTableHeader.css'
import ColumnSelect from "./ColumnSelect";
import { ColumnSpec } from "../../containers/JobTableColumnActions";

type JobTableHeaderProps = {
  queue: string
  jobSet: string
  jobStates: string[]
  newestFirst: boolean
  jobId: string
  canCancel: boolean
  defaultColumns: ColumnSpec[]
  selectedColumns: Set<string>
  onQueueChange: (queue: string) => void
  onJobSetChange: (jobSet: string) => void
  onJobStatesChange: (jobStates: string[]) => void
  onOrderChange: (newestFirst: boolean) => void
  onJobIdChange: (jobId: string) => void
  onRefresh: () => void
  onCancelJobsClick: () => void
  onSelectColumn: (id: string, selected: boolean) => void
}

export default function JobTableHeader(props: JobTableHeaderProps) {
  return (
    <div className="job-table-header">
      <div className="left">
        <h2 className="title">Jobs</h2>
      </div>
      <div className="right">
        <div className="select-columns">
          <ColumnSelect
            selectedColumns={props.selectedColumns}
            defaultColumns={props.defaultColumns}
            additionalColumns={[]}
            inputLabel={"Annotation key"}
            addColumnText={"Add annotation column"}
            onSelect={props.onSelectColumn}
            onDeleteColumn={() => {}}
            onAddColumn={() => {}}
            onChange={() => {}}/>
        </div>
        <div className="cancel-jobs">
          <Button
            disabled={!props.canCancel}
            variant="contained"
            color="secondary"
            startIcon={<CancelIcon/>}
            onClick={props.onCancelJobsClick}>
            Cancel
          </Button>
        </div>
        <div className="refresh">
          <IconButton onClick={props.onRefresh} color="primary">
            <RefreshIcon/>
          </IconButton>
        </div>
      </div>
    </div>
  )
}
