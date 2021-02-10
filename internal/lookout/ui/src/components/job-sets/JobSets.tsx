import React from 'react'
import {
  Container, FormControl,
  IconButton, InputLabel,
  TextField,
  Select, MenuItem, MenuProps,
} from "@material-ui/core";
import RefreshIcon from "@material-ui/icons/Refresh"

import { DurationStats, JOB_STATES_FOR_DISPLAY, JobSet } from "../../services/JobService";

import './JobSets.css'
import JobSetTable from "./JobSetTable";
import { isJobSetsView, JobSetsView } from "../../containers/JobSetsContainer";
import JobDurations from "./JobDurations";

interface JobSetsProps {
  queue: string
  view: JobSetsView
  jobSets: JobSet[]
  onQueueChange: (queue: string) => void
  onViewChange: (view: JobSetsView) => void
  onRefresh: () => void
  onJobSetClick: (jobSet: string, jobState: string) => void
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
};

export default function JobSets(props: JobSetsProps) {
  let content = <JobSetTable jobSets={props.jobSets} onJobSetClick={props.onJobSetClick}/>
  if (props.view === "runtime") {
    content = (
      <JobDurations
        jobSets={props.jobSets.map(js => js.jobSet)}
        durationStats={props.jobSets.filter(js => js.runningStats).map(js => js.runningStats as DurationStats)} />
    )
  }
  if (props.view === "queued-time") {
    content = <div>queued time</div>
  }

  return (
    <Container>
      <div className="job-sets">
        <div className="job-sets-header">
          <h2 className="title">Job Sets</h2>
          <div className="job-sets-params">
            <div className="job-sets-field">
              <TextField
                className="job-sets-field"
                value={props.queue}
                onChange={(event) => {
                  props.onQueueChange(event.target.value)
                }}
                label="Queue"
                variant="outlined"/>
            </div>
            <div className="job-sets-field">
              <FormControl className="job-sets-field">
                <InputLabel htmlFor="view-select">View</InputLabel>
                <Select
                  value={props.view}
                  onChange={(event) => {
                    const value = event.target.value
                    if (typeof value === "string" && isJobSetsView(value)) {
                      props.onViewChange(value)
                    }
                  }}
                  MenuProps={menuProps}>
                  <MenuItem value={"job-counts"}>Job counts</MenuItem>
                  <MenuItem value={"runtime"}>Runtime</MenuItem>
                  <MenuItem value={"queued-time"}>Queued time</MenuItem>
                </Select>
              </FormControl>
            </div>
          </div>
          <div className="refresh-button">
            <IconButton
              title={"Refresh"}
              onClick={props.onRefresh}
              color={"primary"}>
              <RefreshIcon/>
            </IconButton>
          </div>
        </div>
        <div className="job-sets-content">
          {content}
        </div>
      </div>
    </Container>
  )
}
