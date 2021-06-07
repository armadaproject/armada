import React, { Fragment } from "react"

import {
  List,
  ListItem,
  ListItemText,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
} from "@material-ui/core"

import { CancelJobSetsResult } from "../../services/JobService"
import LoadingButton from "../jobs/LoadingButton"

import "./JobSetActions.css"

type CancelJobSetsOutcomeProps = {
  cancelJobSetsResult: CancelJobSetsResult
  isLoading: boolean
  onCancelJobs: () => void
}

export default function CancelJobSetsOutcome(props: CancelJobSetsOutcomeProps) {
  return (
    <div className="job-sets-action-container">
      {props.cancelJobSetsResult.cancelledJobSets.length > 0 && (
        <Fragment>
          <p className="cancel-job-sets-text">The following Job Sets were cancelled successfully:</p>
          <List component={Paper} className="cancel-job-sets-table-container cancel-job-sets-success">
            {props.cancelJobSetsResult.cancelledJobSets.map((jobSet) => (
              <ListItem key={jobSet.jobSetId} className="cancel-job-sets-wrap">
                <ListItemText>{jobSet.jobSetId}</ListItemText>
              </ListItem>
            ))}
          </List>
        </Fragment>
      )}
      {props.cancelJobSetsResult.failedJobSetCancellations.length > 0 && (
        <Fragment>
          <p className="cancel-job-sets-text">Some Job Sets failed to cancel:</p>
          <TableContainer component={Paper} className="cancel-job-sets-table-container">
            <Table stickyHeader className="cancel-job-sets-table">
              <TableHead>
                <TableRow>
                  <TableCell className="cancel-job-sets-id cancel-job-sets-failure-header">Job Set</TableCell>
                  <TableCell className="cancel-job-sets-error cancel-job-sets-failure-header">Error</TableCell>
                </TableRow>
              </TableHead>
              <TableBody className="cancel-job-sets-failure">
                {props.cancelJobSetsResult.failedJobSetCancellations.map((failedCancellation) => (
                  <TableRow key={failedCancellation.jobSet.jobSetId}>
                    <TableCell className="cancel-job-sets-id cancel-job-sets-wrap">
                      {failedCancellation.jobSet.jobSetId}
                    </TableCell>
                    <TableCell className="cancel-job-sets-error cancel-job-sets-wrap">
                      {failedCancellation.error}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
          <div>
            <LoadingButton content={"Retry"} isLoading={props.isLoading} onClick={props.onCancelJobs} />
          </div>
        </Fragment>
      )}
    </div>
  )
}
