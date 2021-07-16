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
          <p className=".job-sets-action-text">The following Job Sets were cancelled successfully:</p>
          <List component={Paper} className="job-sets-action-table-container job-sets-action-success">
            {props.cancelJobSetsResult.cancelledJobSets.map((jobSet) => (
              <ListItem key={jobSet.jobSetId} className="job-sets-action-wrap">
                <ListItemText>{jobSet.jobSetId}</ListItemText>
              </ListItem>
            ))}
          </List>
        </Fragment>
      )}
      {props.cancelJobSetsResult.failedJobSetCancellations.length > 0 && (
        <Fragment>
          <p className="job-sets-action-text">Some Job Sets failed to cancel:</p>
          <TableContainer component={Paper} className="job-sets-action-table-container">
            <Table stickyHeader className="job-sets-action-table">
              <TableHead>
                <TableRow>
                  <TableCell className="job-sets-action-id job-sets-action-failure-header">Job Set</TableCell>
                  <TableCell className="job-sets-action-error job-sets-action-failure-header">Error</TableCell>
                </TableRow>
              </TableHead>
              <TableBody className="job-sets-action-failure">
                {props.cancelJobSetsResult.failedJobSetCancellations.map((failedCancellation) => (
                  <TableRow key={failedCancellation.jobSet.jobSetId}>
                    <TableCell className="job-sets-action-id job-sets-action-wrap">
                      {failedCancellation.jobSet.jobSetId}
                    </TableCell>
                    <TableCell className="job-sets-action-error job-sets-action-wrap">
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
