import { LoadingButton } from "@mui/lab"
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
} from "@mui/material"

import { ReprioritizeJobSetsResponse } from "../../../services/lookout/UpdateJobSetsService"

import "./ReprioritizeJobSets.css"
import "../../Dialog.css"
import "../../Table.css"
import "../../Text.css"

type ReprioritizeJobSetsOutcomeProps = {
  reprioritizeJobSetResponse: ReprioritizeJobSetsResponse
  isLoading: boolean
  newPriority: string
  onReprioritizeJobSets: () => void
}

export default function ReprioritizeJobSetsOutcome({
  reprioritizeJobSetResponse,
  newPriority,
  onReprioritizeJobSets,
  isLoading,
}: ReprioritizeJobSetsOutcomeProps) {
  return (
    <div className="lookout-dialog-container">
      {reprioritizeJobSetResponse.reprioritizedJobSets.length > 0 && (
        <>
          <p className="lookout-dialog-fixed">The following Job Sets were reprioritized successfully:</p>
          <List component={Paper} className="lookout-dialog-varying success">
            {reprioritizeJobSetResponse.reprioritizedJobSets.map((jobSet) => (
              <ListItem key={jobSet.jobSetId} className="lookout-word-wrapped">
                <ListItemText>{jobSet.jobSetId}</ListItemText>
              </ListItem>
            ))}
          </List>
        </>
      )}
      {reprioritizeJobSetResponse.failedJobSetReprioritizations.length > 0 && (
        <>
          <p className="lookout-dialog-fixed">The following Job Sets failed to reprioritize:</p>
          <TableContainer component={Paper} className="lookout-dialog-varying lookout-table-container">
            <Table stickyHeader className="lookout-table">
              <TableHead>
                <TableRow>
                  <TableCell className="reprioritize-job-sets-id failure-header">Job Set</TableCell>
                  <TableCell className="reprioritize-job-sets-error failure-header">Error</TableCell>
                </TableRow>
              </TableHead>
              <TableBody className="failure">
                {reprioritizeJobSetResponse.failedJobSetReprioritizations.map((failedReprioritization) => (
                  <TableRow key={failedReprioritization.jobSet.jobSetId}>
                    <TableCell className="job-sets-action-id lookout-word-wrapped">
                      {failedReprioritization.jobSet.jobSetId}
                    </TableCell>
                    <TableCell className="job-sets-action-error lookout-word-wrapped">
                      {failedReprioritization.error}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
          <div className="lookout-dialog-centered lookout-dialog-fixed">
            <LoadingButton loading={isLoading} variant="contained" onClick={onReprioritizeJobSets}>
              Retry - New priority: {newPriority}
            </LoadingButton>
          </div>
        </>
      )}
    </div>
  )
}
