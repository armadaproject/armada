import React from 'react'
import {
  Container,
  FormControl,
  IconButton,
  InputLabel,
  TextField,
  Select,
  MenuItem,
  MenuProps,
} from "@material-ui/core";
import RefreshIcon from "@material-ui/icons/Refresh"
import { AutoSizer } from "react-virtualized";

import DurationPlotsTable from "./DurationPlotsTable";
import JobSetTable from "./JobSetTable";
import { isJobSetsView, JobSetsView } from "../../containers/JobSetsContainer";
import { DurationStats, JobSet } from "../../services/JobService";

import './JobSets.css'

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
}

export default function JobSets(props: JobSetsProps) {
  let content = (height: number, width: number) => (
    <JobSetTable
      height={height}
      width={width}
      jobSets={props.jobSets}
      onJobSetClick={props.onJobSetClick}/>
  )
  if (props.view === "queued-time") {
    const filtered = props.jobSets.filter(js => js.queuedStats)
    content = (height: number, width: number) => (
      <DurationPlotsTable
        height={height}
        width={width}
        names={filtered.map(js => js.jobSet)}
        durations={filtered.map(js => js.queuedStats as DurationStats)}
        primaryColor={"#00bcd4"}
        secondaryColor={"#673ab7"}
        percentagePlotWidth={0.7}/>
    )
  }
  if (props.view === "runtime") {
    const filtered = props.jobSets.filter(js => js.runningStats)
    content = (height: number, width: number) => (
      <DurationPlotsTable
        height={height}
        width={width}
        names={filtered.map(js => js.jobSet)}
        durations={filtered.map(js => js.runningStats as DurationStats)}
        primaryColor={"#4caf50"}
        secondaryColor={"#3f51b5"}
        percentagePlotWidth={0.7}/>
    )
  }

  return (
    <Container className="job-sets">
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
        <AutoSizer>
          {({ height, width }) => {
            return content(height, width)
          }}
        </AutoSizer>
      </div>
    </Container>
  )
}
