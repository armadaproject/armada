import React from "react"

import { Checkbox, List, ListItem, ListItemText, Paper } from "@material-ui/core"

import { JobSet } from "../../../services/JobService"
import LoadingButton from "../../jobs/LoadingButton"

import "./CancelJobSets.css"
import "../../Dialog.css"
import "../../Text.css"

type CancelJobSetsProps = {
  queue: string
  jobSets: JobSet[]
  isLoading: boolean
  queuedSelected: boolean
  runningSelected: boolean
  onCancelJobSets: () => void
  onQueuedSelectedChange: (queuedSelected: boolean) => void
  onRunningSelectedChange: (runningSelected: boolean) => void
  isPlatformCancel: boolean
  setIsPlatformCancel: (x: boolean) => void
}

export default function CancelJobSets(props: CancelJobSetsProps) {
  return (
    <div className="lookout-dialog-container">
      <p className="lookout-dialog-fixed">The following Job Sets in queue {props.queue} will be cancelled:</p>
      <List component={Paper} className="lookout-dialog-varying cancel-job-sets">
        {props.jobSets.map((jobSet) => (
          <ListItem key={jobSet.jobSetId}>
            <ListItemText className="lookout-word-wrapped">{jobSet.jobSetId}</ListItemText>
          </ListItem>
        ))}
      </List>
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
        <LoadingButton content={"Cancel Job Sets"} isLoading={props.isLoading} onClick={props.onCancelJobSets} />
      </div>
    </div>
  )
}
