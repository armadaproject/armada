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

import { ReprioritizeJobSetsResult } from "../../services/JobService"
import LoadingButton from "../jobs/LoadingButton"

import "./JobSetActions.css"

type ReprioritizeJobSetsOutcomeProps = {
  reprioritizeJobSetResult: ReprioritizeJobSetsResult
  isLoading: boolean
  newPriority: number
  onReprioritizeJobSets: () => void
}

export default function ReprioritizeJobSetsOutcome(props: ReprioritizeJobSetsOutcomeProps) {
  return (
    <div className="job-sets-action-container">
      {props.reprioritizeJobSetResult.reprioritizedJobSets.length > 0 && (
        <Fragment>
          <p className="job-sets-action-text">The following Job Sets were reprioritized successfully:</p>
          <List component={Paper} className="job-sets-action-table-container job-sets-action-success">
            {props.reprioritizeJobSetResult.reprioritizedJobSets.map((jobSet) => (
              <ListItem key={jobSet.jobSetId} className="job-sets-action-wrap">
                <ListItemText>{jobSet.jobSetId}</ListItemText>
              </ListItem>
            ))}
          </List>
        </Fragment>
      )}
      {props.reprioritizeJobSetResult.failedJobSetReprioritizations.length > 0 && (
        <Fragment>
          <p className="job-sets-action-text">The following Job Sets failed to reprioritize:</p>
          <TableContainer component={Paper} className="job-sets-action-table-container">
            <Table stickyHeader className="job-sets-action-table">
              <TableHead>
                <TableRow>
                  <TableCell className="job-sets-action-id job-sets-action-failure-header">Job Set</TableCell>
                  <TableCell className="job-sets-action-error job-sets-action-failure-header">Error</TableCell>
                </TableRow>
              </TableHead>
              <TableBody className="job-sets-action-failure">
                {props.reprioritizeJobSetResult.failedJobSetReprioritizations.map((failedReprioritization) => (
                  <TableRow key={failedReprioritization.jobSet.jobSetId}>
                    <TableCell className="job-sets-action-id job-sets-action-wrap">
                      {failedReprioritization.jobSet.jobSetId}
                    </TableCell>
                    <TableCell className="job-sets-action-error job-sets-action-wrap">
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
