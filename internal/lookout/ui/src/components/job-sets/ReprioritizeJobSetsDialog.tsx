import React from "react"

import { Dialog, DialogContent, DialogTitle } from "@material-ui/core"

import { JobSet, ReprioritizeJobSetsResult } from "../../services/JobService"
import { RequestStatus } from "../../utils"
import ReprioritizeJobSets from "./ReprioritizeJobSets"
import ReprioritizeJobSetsOutcome from "./ReprioritizeJobSetsOutcome"

export type ReprioritizeJobSetsDialogState = "ReprioritizeJobSets" | "ReprioritizeJobSetsResult" | "Closed"

export interface ReprioritizeJobSetsDialogContext {
  dialogState: ReprioritizeJobSetsDialogState
  queue: string
  newPriority: number
  isValid: boolean
  jobSetsToReprioritize: JobSet[]
  reprioritizeJobSetsResult: ReprioritizeJobSetsResult
  reproiritizeJobSetsRequestStatus: RequestStatus
}

interface ReprioritizeJobSetsProps extends ReprioritizeJobSetsDialogContext {
  onReprioritizeJobSets: () => void
  onPriorityChange: (e: string) => void
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
      aria-labelledby="reprioritize-job-sets-dialog-title"
      aria-describedby="reprioritize-job-sets-dialog-description"
      onClose={props.onClose}
      maxWidth={"md"}
    >
      <DialogTitle id="-reprioritize-job-sets-dialog-title">Reprioritize Job Sets</DialogTitle>
      <DialogContent>{content}</DialogContent>
    </Dialog>
  )
}
