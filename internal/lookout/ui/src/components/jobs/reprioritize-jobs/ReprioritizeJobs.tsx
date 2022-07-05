import React from "react"

import { Paper, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, TextField } from "@material-ui/core"

import { Job } from "../../../services/JobService"
import LoadingButton from "../LoadingButton"

import "./ReprioritizeJobs.css"
import "../../Dialog.css"
import "../../Table.css"
import "../../Text.css"

type ReprioritizeJobsProps = {
  jobsToReprioritize: Job[]
  isLoading: boolean
  newPriority: string
  isValid: boolean
  onReprioritizeJobs: () => void
  onPriorityChange: (newPriority: string) => void
}

export default function ReprioritizeJobs(props: ReprioritizeJobsProps) {
  return (
    <div className="lookout-dialog-container">
      <p id="reprioritize-jobs-modal-description" className="lookout-dialog-fixed">
        The following jobs will be reprioritized:
      </p>
      <TableContainer component={Paper} className="lookout-table-container lookout-dialog-varying">
        <Table stickyHeader className="lookout-table">
          <TableHead>
            <TableRow>
              <TableCell className="reprioritize-jobs-cell-job-id">Id</TableCell>
              <TableCell className="reprioritize-jobs-cell-job-set">Job Set</TableCell>
              <TableCell className="reprioritize-jobs-cell-state">State</TableCell>
              <TableCell className="reprioritize-jobs-cell-priority">Current Priority</TableCell>
              <TableCell className="reprioritize-jobs-cell-time">Submission Time</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {props.jobsToReprioritize.map((job) => (
              <TableRow key={job.jobId} className="lookout-word-wrapped">
                <TableCell>{job.jobId}</TableCell>
                <TableCell>{job.jobSet}</TableCell>
                <TableCell>{job.jobState}</TableCell>
                <TableCell>{job.priority}</TableCell>
                <TableCell>{job.submissionTime}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
      <div className="lookout-dialog-fixed lookout-dialog-centered">
        <TextField
          value={props.newPriority}
          autoFocus={true}
          placeholder={"New priority"}
          margin={"normal"}
          type={"text"}
          error={!props.isValid}
          helperText={!props.isValid ? "Value must be a number >= 0" : " "}
          onChange={(event) => props.onPriorityChange(event.target.value)}
        />
      </div>
      <div className="lookout-dialog-fixed lookout-dialog-centered">
        <LoadingButton
          content={"Reprioritize Jobs"}
          isDisabled={!props.isValid}
          isLoading={props.isLoading}
          onClick={props.onReprioritizeJobs}
        />
      </div>
    </div>
  )
}
