import { Button, List, ListItem, ListItemText, Paper, TextField } from "@mui/material"

import { Analytics, ANALYTICS_EVENTS } from "../../../../analytics"
import { JobSet } from "../../../../models/lookoutModels"

import "./ReprioritiseJobSets.css"
import "../Dialog.css"
import "../Text.css"

type ReprioritiseJobSetsProps = {
  queue: string
  jobSets: JobSet[]
  isLoading: boolean
  isValid: boolean
  onReprioritiseJobsSets: () => void
  onPriorityChange: (priority: string) => void
}

export default function ReprioritiseJobSets(props: ReprioritiseJobSetsProps) {
  return (
    <div className="lookout-dialog-container">
      <p className="lookout-dialog-fixed">The following Job Sets in queue {props.queue} will be reprioritised:</p>
      <List component={Paper} className="lookout-dialog-varying reprioritise-job-sets">
        {props.jobSets.map((jobSet) => (
          <ListItem key={jobSet.jobSetId}>
            <ListItemText className="lookout-word-wrapped">{jobSet.jobSetId}</ListItemText>
          </ListItem>
        ))}
      </List>
      <div className="lookout-dialog-centred lookout-dialog-fixed reprioritise-job-sets-options">
        <div>
          <TextField
            autoFocus={true}
            placeholder={"New priority"}
            type={"text"}
            error={!props.isValid}
            helperText={!props.isValid ? "Value must be a number >= 0" : " "}
            onChange={(event) => props.onPriorityChange(event.target.value)}
          />
        </div>
        <div>
          <Analytics
            component={Button}
            eventName={ANALYTICS_EVENTS.REPRIORITISE_JOB_SETS_CLICKED}
            disabled={!props.isValid}
            loading={props.isLoading}
            onClick={props.onReprioritiseJobsSets}
          >
            Reprioritise Job Sets
          </Analytics>
        </div>
      </div>
    </div>
  )
}
