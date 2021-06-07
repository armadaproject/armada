import React from "react"

import { Dialog, DialogContent, DialogTitle } from "@material-ui/core"

import { ReprioritizeJobSetsRequestStatus } from "../../containers/JobSetsContainer"
import { JobSet, ReprioritizeJobSetResult } from "../../services/JobService"
import ReprioritizeJobSets from "./ReprioritizeJobSets"
import ReprioritizeJobSetsOutcome from "./ReprioritizeJobSetsOutcome"

export type ReprioritizeJobSetsDialogState = "ReprioritizeJobSets" | "ReprioritizeJobSetsResult" | "None"

export interface ReprioritizeJobSetsDialogContext {
  dialogState: ReprioritizeJobSetsDialogState
  queue: string
  newPriority: number
  isValid: boolean
  jobSetsToReprioritize: JobSet[]
  reprioritizeJobSetsResult: ReprioritizeJobSetResult
  reproiritizeJobSetsRequestStatus: ReprioritizeJobSetsRequestStatus
}

interface ReprioritizeJobSetsProps extends ReprioritizeJobSetsDialogContext {
  onReprioritizeJobSets: () => void
  onPriorityChange: (e: any) => void
  onClose: () => void
}

export default function ReprioritizeJobSetsDialog(props: ReprioritizeJobSetsProps) {
  const isOpen = props.dialogState === "ReprioritizeJobSets" || props.dialogState === "ReprioritizeJobSetsResult"
  const isLoading = props.reproiritizeJobSetsRequestStatus === "Loading"

  let content = <div />
  if (props.dialogState === "ReprioritizeJobSets") {
    content = (
      <ReprioritizeJobSets
        queue={props.queue}
        jobSets={props.jobSetsToReprioritize}
        isLoading={isLoading}
        isValid={props.isValid}
        onReprioritizeJobsSets={props.onReprioritizeJobSets}
        onPriorityChange={props.onPriorityChange}
      />
    )
  }
  if (props.dialogState === "ReprioritizeJobSetsResult") {
    content = (
      <ReprioritizeJobSetsOutcome
        reprioritizeJobSetResult={props.reprioritizeJobSetsResult}
        isLoading={isLoading}
        newPriority={props.newPriority}
        onReprioritizeJobSets={props.onReprioritizeJobSets}
      />
    )
  }

  return (
    <Dialog
      open={isOpen}
      aria-labelledby="cancel-job-sets-modal-title"
      aria-describedby="cancel-job-sets-modal-description"
      onClose={props.onClose}
      maxWidth={"md"}
    >
      <DialogTitle id="cancel-job-sets-dialog-title">Reprioritize Job Sets</DialogTitle>
      <DialogContent>{content}</DialogContent>
    </Dialog>
  )
}
