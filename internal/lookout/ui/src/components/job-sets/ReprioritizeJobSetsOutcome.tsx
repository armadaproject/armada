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

import { ReprioritizeJobSetResult } from "../../services/JobService"
import LoadingButton from "../jobs/LoadingButton"

import "./CancelJobSets.css"

type ReprioritizeJobSetsOutcomeProps = {
  reprioritizeJobSetResult: ReprioritizeJobSetResult
  isLoading: boolean
  newPriority: number
  onReprioritizeJobSets: () => void
}

export default function ReprioritizeJobSetsOutcome(props: ReprioritizeJobSetsOutcomeProps) {
  return (
    <div className="cancel-job-sets-container">
      {props.reprioritizeJobSetResult.reprioritizedJobSets.length > 0 && (
        <Fragment>
          <p className="cancel-job-sets-text">The following Job Sets were reprioritized successfully:</p>
          <List component={Paper} className="cancel-job-sets-table-container cancel-job-sets-success">
            {props.reprioritizeJobSetResult.reprioritizedJobSets.map((jobSet) => (
              <ListItem key={jobSet.jobSetId} className="cancel-job-sets-wrap">
                <ListItemText>{jobSet.jobSetId}</ListItemText>
              </ListItem>
            ))}
          </List>
        </Fragment>
      )}
      {props.reprioritizeJobSetResult.failedJobSetReprioritizations.length > 0 && (
        <Fragment>
          <p className="cancel-job-sets-text">The following Job Sets failed to reprioritize:</p>
          <TableContainer component={Paper} className="cancel-job-sets-table-container">
            <Table stickyHeader className="cancel-job-sets-table">
              <TableHead>
                <TableRow>
                  <TableCell className="cancel-job-sets-id cancel-job-sets-failure-header">Job Set</TableCell>
                  <TableCell className="cancel-job-sets-error cancel-job-sets-failure-header">Error</TableCell>
                </TableRow>
              </TableHead>
              <TableBody className="cancel-job-sets-failure">
                {props.reprioritizeJobSetResult.failedJobSetReprioritizations.map((failedReprioritization) => (
                  <TableRow key={failedReprioritization.jobSet.jobSetId}>
                    <TableCell className="cancel-job-sets-id cancel-job-sets-wrap">
                      {failedReprioritization.jobSet.jobSetId}
                    </TableCell>
                    <TableCell className="cancel-job-sets-error cancel-job-sets-wrap">
                      {failedReprioritization.error}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
          <div>
            <LoadingButton
              content={"Retry - New priority " + props.newPriority}
              isLoading={props.isLoading}
              onClick={props.onReprioritizeJobSets}
            />
          </div>
        </Fragment>
      )}
    </div>
  )
}
