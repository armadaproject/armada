import React from "react"

import { Button } from "@material-ui/core"
import CancelIcon from "@material-ui/icons/Cancel"
import LowPriority from "@material-ui/icons/LowPriority"

import ColumnSelect from "./ColumnSelect"
import { ColumnSpec } from "../../containers/JobsContainer"
import AutoRefreshToggle from "../AutoRefreshToggle"
import RefreshButton from "../RefreshButton"

import "./JobTableHeader.css"

type JobTableHeaderProps = {
  defaultColumns: ColumnSpec<string | boolean | string[]>[]
  annotationColumns: ColumnSpec<string>[]
  canCancel: boolean
  autoRefresh: boolean
  canReprioritize: boolean
  isLoading: boolean
  onRefresh: () => void
  onAutoRefreshChange: (autoRefresh: boolean) => void
  onCancelJobsClick: () => void
  onReprioritizeJobsClick: () => void
  onDisableColumn: (id: string, isDisabled: boolean) => void
  onDeleteColumn: (columnId: string) => void
  onAddColumn: () => void
  onChangeAnnotationColumnKey: (columnId: string, newKey: string) => void
  onClear: () => void
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
            defaultColumns={props.defaultColumns}
            annotationColumns={props.annotationColumns}
            inputLabel={"Annotation key"}
            addColumnText={"Add column for annotation"}
            onDisableColumn={props.onDisableColumn}
            onDeleteColumn={props.onDeleteColumn}
            onAddColumn={props.onAddColumn}
            onChangeAnnotationColumnKey={props.onChangeAnnotationColumnKey}
          />
        </div>
        <div className="reprioritize-jobs">
          <Button
            disabled={!props.canReprioritize}
            variant="contained"
            color="primary"
            onClick={props.onReprioritizeJobsClick}
            startIcon={<LowPriority />}
          >
            Reprioritize
          </Button>
        </div>
        <div className="cancel-jobs">
          <Button
            disabled={!props.canCancel}
            variant="contained"
            color="secondary"
            startIcon={<CancelIcon />}
            onClick={props.onCancelJobsClick}
          >
            Cancel
          </Button>
        </div>
        <div className="clear">
          <Button onClick={props.onClear} color="secondary">
            Clear Search
          </Button>
        </div>
        <div className="auto-refresh">
          <AutoRefreshToggle autoRefresh={props.autoRefresh} onAutoRefreshChange={props.onAutoRefreshChange} />
        </div>
        <div className="refresh">
          <RefreshButton isLoading={props.isLoading} onClick={props.onRefresh} />
        </div>
      </div>
    </div>
  )
}
