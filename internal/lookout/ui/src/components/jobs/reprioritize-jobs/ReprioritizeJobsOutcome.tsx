import React, { Fragment } from "react"

import { Paper, Table, TableBody, TableCell, TableContainer, TableHead, TableRow } from "@material-ui/core"

import { ReprioritizeJobsResult } from "../../../services/JobService"
import LoadingButton from "../LoadingButton"

import "./ReprioritizeJobsOutcome.css"
import "../../Dialog.css"
import "../../Table.css"
import "../../Text.css"

type ReprioritizeJobsOutcomeProps = {
  reprioritizeJobsResult: ReprioritizeJobsResult
  newPriority: string
  isLoading: boolean
  onReprioritizeJobs: () => void
}

export default function (props: ReprioritizeJobsOutcomeProps) {
  return (
    <div className="lookout-dialog-container">
      {props.reprioritizeJobsResult.reprioritizedJobs.length > 0 && (
        <Fragment>
          <p className="lookout-dialog-fixed">The following jobs were reprioritized successfully:</p>
          <TableContainer component={Paper} className="lookout-table-container lookout-dialog-varying">
            <Table stickyHeader className="lookout-table">
              <TableHead>
                <TableRow>
                  <TableCell className="success-header reprioritize-jobs-cell-success-job-id">Id</TableCell>
                  <TableCell className="success-header reprioritize-jobs-cell-success-job-set">Job Set</TableCell>
                  <TableCell className="success-header reprioritize-jobs-cell-success-time">Submission Time</TableCell>
                </TableRow>
              </TableHead>
              <TableBody className="success">
                {props.reprioritizeJobsResult.reprioritizedJobs.map((job) => (
                  <TableRow key={job.jobId} className="lookout-word-wrapped">
                    <TableCell>{job.jobId}</TableCell>
                    <TableCell>{job.jobSet}</TableCell>
                    <TableCell>{job.submissionTime}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        </Fragment>
      )}
      {props.reprioritizeJobsResult.failedJobReprioritizations.length > 0 && (
        <Fragment>
          <p id="reprioritize-jobs-modal-description" className="lookout-dialog-fixed">
            Failed to reprioritize the following jobs:
          </p>
          <TableContainer component={Paper} className="lookout-dialog-varying">
            <Table stickyHeader className="lookout-table">
              <TableHead>
                <TableRow>
                  <TableCell className="failure-header reprioritize-jobs-cell-failure-job-id">Id</TableCell>
                  <TableCell className="failure-header reprioritize-jobs-cell-failure-job-set">Job Set</TableCell>
                  <TableCell className="failure-header reprioritize-jobs-cell-failure-priority">
                    Current Priority
                  </TableCell>
                  <TableCell className="failure-header reprioritize-jobs-cell-failure-time">Submission Time</TableCell>
                  <TableCell className="failure-header reprioritize-jobs-cell-failure-error">Error</TableCell>
                </TableRow>
              </TableHead>
              <TableBody className="failure">
                {props.reprioritizeJobsResult.failedJobReprioritizations.map((failed) => (
                  <TableRow key={failed.job.jobId} className="lookout-word-wrapped">
                    <TableCell>{failed.job.jobId}</TableCell>
                    <TableCell>{failed.job.jobSet}</TableCell>
                    <TableCell>{failed.job.priority}</TableCell>
                    <TableCell>{failed.job.submissionTime}</TableCell>
                    <TableCell>{failed.error}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
          <div className="lookout-dialog-centered lookout-dialog-fixed">
            <LoadingButton
              content={`Retry - New priority: ${props.newPriority}`}
              isLoading={props.isLoading}
              onClick={props.onReprioritizeJobs}
            />
          </div>
        </Fragment>
      )}
    </div>
  )
}
