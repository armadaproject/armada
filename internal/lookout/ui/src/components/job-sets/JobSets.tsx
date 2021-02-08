import React from 'react'
import {
  Container,
  IconButton,
  TextField,
} from "@material-ui/core";
import RefreshIcon from "@material-ui/icons/Refresh"

import { JobSet } from "../../services/JobService";

import './JobSets.css'
import JobSetTable from "./JobSetTable";

interface JobSetsProps {
  queue: string
  jobSets: JobSet[]
  onQueueChange: (queue: string) => void
  onRefresh: () => void
  onJobSetClick: (jobSet: string, jobState: string) => void
}

export default function JobSets(props: JobSetsProps) {
  return (
    <Container className="job-sets">
      <div className="job-sets-header">
        <h2 className="title">Job Sets</h2>
        <TextField
          value={props.queue}
          onChange={(event) => {
            props.onQueueChange(event.target.value)
          }}
          label="Queue"
          variant="outlined" />
        <div className="refresh-button">
          <IconButton
            title={"Refresh"}
            onClick={props.onRefresh}
            color={"primary"}>
            <RefreshIcon />
          </IconButton>
        </div>
      </div>
      <div className="job-sets-content">
        <JobSetTable jobSets={props.jobSets} onJobSetClick={props.onJobSetClick} />
      </div>
    </Container>
  )
}
