import {
  Button,
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

import { ReprioritiseJobSetsResponse } from "../../../../services/lookout/useReprioritiseJobSets"

import "./ReprioritiseJobSets.css"
import "../Dialog.css"
import "../Table.css"
import "../Text.css"

type ReprioritiseJobSetsOutcomeProps = {
  reprioritiseJobSetResponse: ReprioritiseJobSetsResponse
  isLoading: boolean
  newPriority: string
  onReprioritiseJobSets: () => void
}

export default function ReprioritiseJobSetsOutcome({
  reprioritiseJobSetResponse,
  newPriority,
  onReprioritiseJobSets,
  isLoading,
}: ReprioritiseJobSetsOutcomeProps) {
  return (
    <div className="lookout-dialog-container">
      {reprioritiseJobSetResponse.reprioritisedJobSets.length > 0 && (
        <>
          <p className="lookout-dialog-fixed">The following Job Sets were reprioritised successfully:</p>
          <List component={Paper} className="lookout-dialog-varying success">
            {reprioritiseJobSetResponse.reprioritisedJobSets.map((jobSet) => (
              <ListItem key={jobSet.jobSetId} className="lookout-word-wrapped">
                <ListItemText>{jobSet.jobSetId}</ListItemText>
              </ListItem>
            ))}
          </List>
        </>
      )}
      {reprioritiseJobSetResponse.failedJobSetReprioritisations.length > 0 && (
        <>
          <p className="lookout-dialog-fixed">The following Job Sets failed to reprioritise:</p>
          <TableContainer component={Paper} className="lookout-dialog-varying lookout-table-container">
            <Table stickyHeader className="lookout-table">
              <TableHead>
                <TableRow>
                  <TableCell className="reprioritise-job-sets-id failure-header">Job Set</TableCell>
                  <TableCell className="reprioritise-job-sets-error failure-header">Error</TableCell>
                </TableRow>
              </TableHead>
              <TableBody className="failure">
                {reprioritiseJobSetResponse.failedJobSetReprioritisations.map((failedReprioritisation) => (
                  <TableRow key={failedReprioritisation.jobSet.jobSetId}>
                    <TableCell className="job-sets-action-id lookout-word-wrapped">
                      {failedReprioritisation.jobSet.jobSetId}
                    </TableCell>
                    <TableCell className="job-sets-action-error lookout-word-wrapped">
                      {failedReprioritisation.error}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
          <div className="lookout-dialog-centred lookout-dialog-fixed">
            <Button loading={isLoading} variant="contained" onClick={onReprioritiseJobSets}>
              Retry - New priority: {newPriority}
            </Button>
          </div>
        </>
      )}
    </div>
  )
}
