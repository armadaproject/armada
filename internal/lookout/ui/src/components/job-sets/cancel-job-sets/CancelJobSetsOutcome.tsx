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

import { CancelJobSetsResponse } from "../../../services/JobService"
import LoadingButton from "../../jobs/LoadingButton"

import "./CancelJobSets.css"
import "../../Dialog.css"
import "../../Table.css"
import "../../Text.css"

type CancelJobSetsOutcomeProps = {
  cancelJobSetsResult: CancelJobSetsResponse
  isLoading: boolean
  onCancelJobs: () => void
}

export default function CancelJobSetsOutcome(props: CancelJobSetsOutcomeProps) {
  return (
    <div className="lookout-dialog-container">
      {props.cancelJobSetsResult.cancelledJobSets.length > 0 && (
        <>
          <p className="lookout-dialog-fixed">The following Job Sets were cancelled successfully:</p>
          <List component={Paper} className="lookout-dialog-varying cancel-job-sets success">
            {props.cancelJobSetsResult.cancelledJobSets.map((jobSet) => (
              <ListItem key={jobSet.jobSetId} className="lookout-word-wrapped">
                <ListItemText>{jobSet.jobSetId}</ListItemText>
              </ListItem>
            ))}
          </List>
        </>
      )}
      {props.cancelJobSetsResult.failedJobSetCancellations.length > 0 && (
        <Fragment>
          <p className="lookout-dialog-fixed">Some Job Sets failed to cancel:</p>
          <TableContainer component={Paper} className="lookout-dialog-varying lookout-table-container">
            <Table stickyHeader className="lookout-table">
              <TableHead>
                <TableRow>
                  <TableCell className="cancel-job-sets-id failure-header">Job Set</TableCell>
                  <TableCell className="cancel-job-sets-error failure-header">Error</TableCell>
                </TableRow>
              </TableHead>
              <TableBody className="failure">
                {props.cancelJobSetsResult.failedJobSetCancellations.map((failedCancellation) => (
                  <TableRow key={failedCancellation.jobSet.jobSetId}>
                    <TableCell className="lookout-word-wrapped">{failedCancellation.jobSet.jobSetId}</TableCell>
                    <TableCell className="lookout-word-wrapped">{failedCancellation.error}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
          <div className="lookout-dialog-centered lookout-dialog-fixed">
            <LoadingButton content={"Retry"} isLoading={props.isLoading} onClick={props.onCancelJobs} />
          </div>
        </Fragment>
      )}
    </div>
  )
}
