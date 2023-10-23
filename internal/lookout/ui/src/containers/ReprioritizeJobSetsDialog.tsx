import React, { useState } from "react"

import { Dialog, DialogContent, DialogTitle } from "@material-ui/core"

import ReprioritizeJobSets from "../components/job-sets/reprioritize-job-sets/ReprioritizeJobSets"
import ReprioritizeJobSetsOutcome from "../components/job-sets/reprioritize-job-sets/ReprioritizeJobSetsOutcome"
import { JobSet } from "../services/JobService"
import { ReprioritizeJobSetsResponse, UpdateJobSetsService } from "../services/lookoutV2/UpdateJobSetsService"
import { ApiResult, priorityIsValid, RequestStatus } from "../utils"

import "../components/Dialog.css"

export type ReprioritizeJobSetsDialogState = "ReprioritizeJobSets" | "ReprioritizeJobSetsResult"

type ReprioritizeJobSetsDialogProps = {
  isOpen: boolean
  queue: string
  selectedJobSets: JobSet[]
  updateJobSetsService: UpdateJobSetsService
  onResult: (result: ApiResult) => void
  onClose: () => void
}

export function getReprioritizeableJobSets(jobSets: JobSet[]): JobSet[] {
  return jobSets.filter((jobSet) => jobSet.jobsQueued > 0)
}

export default function ReprioritizeJobSetsDialog(props: ReprioritizeJobSetsDialogProps) {
  const [state, setState] = useState<ReprioritizeJobSetsDialogState>("ReprioritizeJobSets")
  const [response, setResponse] = useState<ReprioritizeJobSetsResponse>({
    reprioritizedJobSets: [],
    failedJobSetReprioritizations: [],
  })
  const [requestStatus, setRequestStatus] = useState<RequestStatus>("Idle")
  const [priority, setPriority] = useState<string>("")

  const jobSetsToReprioritize = getReprioritizeableJobSets(props.selectedJobSets)

  async function reprioritizeJobSets() {
    if (requestStatus == "Loading" || !priorityIsValid(priority)) {
      return
    }

    setRequestStatus("Loading")
    const reprioritizeJobSetsResponse = await props.updateJobSetsService.reprioritizeJobSets(
      props.queue,
      jobSetsToReprioritize,
      Number(priority),
    )
    setRequestStatus("Idle")

    setResponse(reprioritizeJobSetsResponse)
    setState("ReprioritizeJobSetsResult")
    if (reprioritizeJobSetsResponse.failedJobSetReprioritizations.length === 0) {
      props.onResult("Success")
    } else if (reprioritizeJobSetsResponse.reprioritizedJobSets.length === 0) {
      props.onResult("Failure")
    } else {
      props.onResult("Partial success")
    }
  }

  function cleanup() {
    setPriority("")
    setState("ReprioritizeJobSets")
    setResponse({
      reprioritizedJobSets: [],
      failedJobSetReprioritizations: [],
    })
  }

  return (
    <Dialog
      open={props.isOpen}
      aria-labelledby="reprioritize-job-sets-dialog-title"
      aria-describedby="reprioritize-job-sets-dialog-description"
      onClose={props.onClose}
      TransitionProps={{
        onExited: cleanup,
      }}
      maxWidth={"md"}
    >
      <DialogTitle id="-reprioritize-job-sets-dialog-title">Reprioritize Job Sets</DialogTitle>
      <DialogContent className="lookout-dialog">
        {state === "ReprioritizeJobSets" && (
          <ReprioritizeJobSets
            queue={props.queue}
            jobSets={jobSetsToReprioritize}
            isLoading={requestStatus === "Loading"}
            isValid={priorityIsValid(priority)}
            onReprioritizeJobsSets={reprioritizeJobSets}
            onPriorityChange={setPriority}
          />
        )}
        {state === "ReprioritizeJobSetsResult" && (
          <ReprioritizeJobSetsOutcome
            reprioritizeJobSetResponse={response}
            isLoading={requestStatus === "Loading"}
            newPriority={priority}
            onReprioritizeJobSets={reprioritizeJobSets}
          />
        )}
      </DialogContent>
    </Dialog>
  )
}
