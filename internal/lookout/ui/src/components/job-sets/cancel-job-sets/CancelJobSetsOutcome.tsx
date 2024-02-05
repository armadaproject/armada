import React from "react"

import {
  Checkbox,
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

import { CancelJobSetsResponse } from "../../../services/lookoutV2/UpdateJobSetsService"
import LoadingButton from "../../jobs/LoadingButton"

import "./CancelJobSets.css"
import "../../Dialog.css"
import "../../Table.css"
import "../../Text.css"

type CancelJobSetsOutcomeProps = {
  cancelJobSetsResponse: CancelJobSetsResponse
  isLoading: boolean
  queuedSelected: boolean
  runningSelected: boolean
  onCancelJobs: () => void
  onQueuedSelectedChange: (queuedSelected: boolean) => void
  onRunningSelectedChange: (runningSelected: boolean) => void
  isPlatformCancel: boolean
  setIsPlatformCancel: (x: boolean) => void
}

export default function CancelJobSetsOutcome(props: CancelJobSetsOutcomeProps) {
  return (
    <div className="lookout-dialog-container">
      {props.cancelJobSetsResponse.cancelledJobSets.length > 0 && (
        <>
          <p className="lookout-dialog-fixed">The following Job Sets were cancelled successfully:</p>
          <List component={Paper} className="lookout-dialog-varying cancel-job-sets success">
            {props.cancelJobSetsResponse.cancelledJobSets.map((jobSet) => (
              <ListItem key={jobSet.jobSetId} className="lookout-word-wrapped">
                <ListItemText>{jobSet.jobSetId}</ListItemText>
              </ListItem>
            ))}
          </List>
        </>
      )}
      {props.cancelJobSetsResponse.failedJobSetCancellations.length > 0 && (
        <>
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
                {props.cancelJobSetsResponse.failedJobSetCancellations.map((failedCancellation) => (
                  <TableRow key={failedCancellation.jobSet.jobSetId}>
                    <TableCell className="lookout-word-wrapped">{failedCancellation.jobSet.jobSetId}</TableCell>
                    <TableCell className="lookout-word-wrapped">{failedCancellation.error}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
          <div>
            <Checkbox
              checked={props.queuedSelected}
              onChange={(event) => props.onQueuedSelectedChange(event.target.checked)}
            />
            <label>Queued</label>
          </div>
          <div>
            <Checkbox
              checked={props.runningSelected}
              onChange={(event) => props.onRunningSelectedChange(event.target.checked)}
            />
            <label>Pending + Running</label>
          </div>
          <div>
            <label>Is Platform error?</label>
            <Checkbox
              checked={props.isPlatformCancel}
              disabled={props.isLoading}
              onChange={(event) => props.setIsPlatformCancel(event.target.checked)}
            />
          </div>
          <div className="lookout-dialog-centered lookout-dialog-fixed">
            <LoadingButton content={"Retry"} isLoading={props.isLoading} onClick={props.onCancelJobs} />
          </div>
        </>
      )}
    </div>
  )
}
