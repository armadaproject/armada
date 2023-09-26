import React from "react"

import {
  Button,
  Container,
  FormControl,
  InputLabel,
  MenuItem,
  MenuProps,
  Select,
  TextField,
  FormControlLabel,
  Checkbox,
  Tooltip,
} from "@material-ui/core"
import CancelIcon from "@material-ui/icons/Cancel"
import LowPriority from "@material-ui/icons/LowPriority"
import { AutoSizer } from "react-virtualized"

import DurationPlotsTable from "./DurationPlotsTable"
import JobSetTable from "./JobSetTable"
import { JobSetsView, isJobSetsView } from "../../containers/JobSetsContainer"
import { DurationStats, JobSet } from "../../services/JobService"
import { RequestStatus } from "../../utils"
import AutoRefreshToggle from "../AutoRefreshToggle"
import RefreshButton from "../RefreshButton"

import "./JobSets.css"

interface JobSetsProps {
  queue: string
  view: JobSetsView
  jobSets: JobSet[]
  selectedJobSets: Map<string, JobSet>
  canCancel: boolean
  getJobSetsRequestStatus: RequestStatus
  autoRefresh: boolean
  canReprioritize: boolean
  newestFirst: boolean
  activeOnly: boolean
  onQueueChange: (queue: string) => void
  onViewChange: (view: JobSetsView) => void
  onRefresh: () => void
  onJobSetClick: (jobSet: string, jobState: string) => void
  onSelectJobSet: (index: number, selected: boolean) => void
  onShiftSelectJobSet: (index: number, selected: boolean) => void
  onDeselectAllClick: () => void
  onSelectAllClick: () => void
  onCancelJobSetsClick: () => void
  onToggleAutoRefresh: (autoRefresh: boolean) => void
  onReprioritizeJobSetsClick: () => void
  onOrderChange: (newestFirst: boolean) => void
  onActiveOnlyChange: (activeOnly: boolean) => void
}

const menuProps: Partial<MenuProps> = {
  anchorOrigin: {
    vertical: "bottom",
    horizontal: "left",
  },
  transformOrigin: {
    vertical: "top",
    horizontal: "left",
  },
  getContentAnchorEl: null,
}

export default function JobSets(props: JobSetsProps) {
  let content = (height: number, width: number) => (
    <JobSetTable
      height={height}
      width={width}
      jobSets={props.jobSets}
      selectedJobSets={props.selectedJobSets}
      newestFirst={props.newestFirst}
      onJobSetClick={props.onJobSetClick}
      onSelectJobSet={props.onSelectJobSet}
      onShiftSelectJobSet={props.onShiftSelectJobSet}
      onDeselectAllClick={props.onDeselectAllClick}
      onSelectAllClick={props.onSelectAllClick}
      onOrderChange={props.onOrderChange}
    />
  )
  if (props.view === "queued-time") {
    const filtered = props.jobSets.filter((js) => js.queuedStats)
    content = (height: number, width: number) => (
      <DurationPlotsTable
        height={height}
        width={width}
        names={filtered.map((js) => js.jobSetId)}
        durations={filtered.map((js) => js.queuedStats as DurationStats)}
        primaryColor={"#00bcd4"}
        secondaryColor={"#673ab7"}
        percentagePlotWidth={0.7}
      />
    )
  }
  if (props.view === "runtime") {
    const filtered = props.jobSets.filter((js) => js.runningStats)
    content = (height: number, width: number) => (
      <DurationPlotsTable
        height={height}
        width={width}
        names={filtered.map((js) => js.jobSetId)}
        durations={filtered.map((js) => js.runningStats as DurationStats)}
        primaryColor={"#4caf50"}
        secondaryColor={"#3f51b5"}
        percentagePlotWidth={0.7}
      />
    )
  }

  return (
    <Container className="job-sets" maxWidth={false}>
      <div className="job-sets-header">
        <div className="job-sets-params">
          <h2 className="title">Job Sets</h2>
          <div className="job-sets-field">
            <TextField
              className="job-sets-field"
              value={props.queue}
              onChange={(event) => {
                props.onQueueChange(event.target.value)
              }}
              label="Queue"
              variant="outlined"
            />
          </div>
          <div className="job-sets-field">
            <FormControl className="job-sets-field">
              <InputLabel>View</InputLabel>
              <Select
                value={props.view}
                onChange={(event) => {
                  const value = event.target.value
                  if (typeof value === "string" && isJobSetsView(value)) {
                    props.onViewChange(value)
                  }
                }}
                MenuProps={menuProps}
              >
                <MenuItem value={"job-counts"}>Job counts</MenuItem>
                <MenuItem value={"runtime"}>Runtime</MenuItem>
                <MenuItem value={"queued-time"}>Queued time</MenuItem>
              </Select>
            </FormControl>
          </div>
          <div className="job-sets-field">
            <Tooltip title="Only display Queued, Pending or Running">
              <FormControlLabel
                control={
                  <Checkbox
                    color="primary"
                    checked={props.activeOnly}
                    onChange={(event) => {
                      props.onActiveOnlyChange(event.target.checked)
                    }}
                  />
                }
                label="Active only"
                labelPlacement="end"
              />
            </Tooltip>
          </div>
        </div>
        <div className="job-sets-actions">
          <div className="reprioritize-button">
            <Button
              disabled={!props.canReprioritize}
              variant="contained"
              color="primary"
              startIcon={<LowPriority />}
              onClick={props.onReprioritizeJobSetsClick}
            >
              Reprioritize
            </Button>
          </div>
          <div className="cancel-button">
            <Button
              disabled={!props.canCancel}
              variant="contained"
              color="secondary"
              startIcon={<CancelIcon />}
              onClick={props.onCancelJobSetsClick}
            >
              Cancel
            </Button>
          </div>
          <div className="auto-refresh">
            <AutoRefreshToggle autoRefresh={props.autoRefresh} onAutoRefreshChange={props.onToggleAutoRefresh} />
          </div>
          <div className="refresh-button">
            <RefreshButton isLoading={props.getJobSetsRequestStatus === "Loading"} onClick={props.onRefresh} />
          </div>
        </div>
      </div>
      <div className="job-sets-content">
        <AutoSizer>
          {({ height, width }) => {
            return content(height, width)
          }}
        </AutoSizer>
      </div>
    </Container>
  )
}
