import React from "react"

import { List, ListItem, ListItemText, Paper } from "@material-ui/core"

import { JobSet } from "../../services/JobService"
import LoadingButton from "../jobs/LoadingButton"

import "./JobSetActions.css"

type CancelJobSetsProps = {
  queue: string
  jobSets: JobSet[]
  isLoading: boolean
  onCancelJobSets: () => void
}

export default function CancelJobSets(props: CancelJobSetsProps) {
  return (
    <div className="job-sets-action-container">
      <p className="job-sets-action-text">The following Job Sets in queue {props.queue} will be cancelled:</p>
      <List component={Paper} className="job-sets-action-table-container">
        {props.jobSets.map((jobSet) => (
          <ListItem key={jobSet.jobSetId}>
            <ListItemText className="cancel-job-sets-wrap">{jobSet.jobSetId}</ListItemText>
          </ListItem>
        ))}
      </List>
      <div>
        <LoadingButton content={"Cancel Job Sets"} isLoading={props.isLoading} onClick={props.onCancelJobSets} />
      </div>
    </div>
  )
}
