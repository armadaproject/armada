import React from "react"

import { List, ListItem, ListItemText, Paper, TextField } from "@material-ui/core"

import { JobSet } from "../../services/JobService"
import LoadingButton from "../jobs/LoadingButton"

import "./CancelJobSets.css"

type ReprioritizeJobSetsProps = {
  queue: string
  jobSets: JobSet[]
  isLoading: boolean
  isValid: boolean
  onReprioritizeJobsSets: () => void
  onPriorityChange: (e: any) => void
}

export default function ReprioritizeJobSets(props: ReprioritizeJobSetsProps) {
  return (
    <div className="cancel-job-sets-container">
      <p className="cancel-job-sets-text">The following Job Sets in queue {props.queue} will be reprioritized:</p>
      <List component={Paper} className="cancel-job-sets-table-container">
        {props.jobSets.map((jobSet) => (
          <ListItem key={jobSet.jobSetId}>
            <ListItemText className="cancel-job-sets-wrap">{jobSet.jobSetId}</ListItemText>
          </ListItem>
        ))}
      </List>
      <div>
        <TextField
          autoFocus={true}
          placeholder={"New priority"}
          margin={"normal"}
          type={"text"}
          error={!props.isValid}
          helperText={!props.isValid ? "Value must be a number >= 0" : " "}
          onChange={props.onPriorityChange}
        />
        <LoadingButton
          content={"Reprioritize Job Sets"}
          isDisabled={!props.isValid}
          isLoading={props.isLoading}
          onClick={props.onReprioritizeJobsSets}
        />
      </div>
    </div>
  )
}
