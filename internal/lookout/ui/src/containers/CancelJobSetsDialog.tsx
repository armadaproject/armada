import React, { useState } from "react"

import { Dialog, DialogContent, DialogTitle } from "@material-ui/core"

import CancelJobSets from "../components/job-sets/cancel-job-sets/CancelJobSets"
import CancelJobSetsOutcome from "../components/job-sets/cancel-job-sets/CancelJobSetsOutcome"
import JobService, { CancelJobSetsResponse, JobSet } from "../services/JobService"
import { ApiResult, RequestStatus } from "../utils"

export type CancelJobSetsDialogState = "CancelJobSets" | "CancelJobSetsResult"

type CancelJobSetsDialogProps = {
  isOpen: boolean
  queue: string
  selectedJobSets: JobSet[]
  jobService: JobService
  onResult: (result: ApiResult) => void
  onClose: () => void
}

export function getCancellableJobSets(jobSets: JobSet[]): JobSet[] {
  return jobSets.filter((jobSet) => jobSet.jobsQueued > 0 || jobSet.jobsPending > 0 || jobSet.jobsRunning > 0)
}

export default function CancelJobSetsDialog(props: CancelJobSetsDialogProps) {
  const [state, setState] = useState<CancelJobSetsDialogState>("CancelJobSets")
  const [response, setResponse] = useState<CancelJobSetsResponse>({
    cancelledJobSets: [],
    failedJobSetCancellations: [],
  })
  const [requestStatus, setRequestStatus] = useState<RequestStatus>("Idle")

  const jobSetsToCancel = getCancellableJobSets(props.selectedJobSets)

  async function cancelJobSets() {
    if (requestStatus === "Loading") {
      return
    }

    setRequestStatus("Loading")
    const cancelJobSetsResponse = await props.jobService.cancelJobSets(props.queue, jobSetsToCancel)
    setRequestStatus("Idle")

    setResponse(cancelJobSetsResponse)
    setState("CancelJobSetsResult")
    if (cancelJobSetsResponse.failedJobSetCancellations.length === 0) {
      props.onResult("Success")
    } else if (cancelJobSetsResponse.cancelledJobSets.length === 0) {
      props.onResult("Failure")
    } else {
      props.onResult("Partial success")
    }
  }

  function close() {
    props.onClose()
    setState("CancelJobSets")
    setResponse({
      cancelledJobSets: [],
      failedJobSetCancellations: [],
    })
  }

  return (
    <Dialog
      open={props.isOpen}
      aria-labelledby="cancel-job-sets-modal-title"
      aria-describedby="cancel-job-sets-modal-description"
      onClose={close}
      maxWidth={"md"}
    >
      <DialogTitle id="cancel-job-sets-dialog-title">Cancel Job Sets</DialogTitle>
      <DialogContent className="lookout-dialog">
        {state === "CancelJobSets" && (
          <CancelJobSets
            queue={props.queue}
            jobSets={jobSetsToCancel}
            isLoading={requestStatus === "Loading"}
            onCancelJobSets={cancelJobSets}
          />
        )}
        {state === "CancelJobSetsResult" && (
          <CancelJobSetsOutcome
            cancelJobSetsResponse={response}
            isLoading={requestStatus === "Loading"}
            onCancelJobs={cancelJobSets}
          />
        )}
      </DialogContent>
    </Dialog>
  )
}
