import React, { useState } from "react"

import { Dialog, DialogContent, DialogTitle } from "@material-ui/core"

import CancelJobSets from "../components/job-sets/cancel-job-sets/CancelJobSets"
import CancelJobSetsOutcome from "../components/job-sets/cancel-job-sets/CancelJobSetsOutcome"
import { ApiJobState } from "../openapi/armada"
import { JobSet } from "../services/JobService"
import { CancelJobSetsResponse, UpdateJobSetsService } from "../services/lookoutV2/UpdateJobSetsService"
import { ApiResult, PlatformCancelReason, RequestStatus } from "../utils"

export type CancelJobSetsDialogState = "CancelJobSets" | "CancelJobSetsResult"

type CancelJobSetsDialogProps = {
  isOpen: boolean
  queue: string
  selectedJobSets: JobSet[]
  updateJobSetsService: UpdateJobSetsService
  onResult: (result: ApiResult) => void
  onClose: () => void
}

export function getCancellableJobSets(jobSets: JobSet[]): JobSet[] {
  return jobSets.filter((jobSet) => jobSet.jobsQueued > 0 || jobSet.jobsPending > 0 || jobSet.jobsRunning > 0)
}

export function getStatesToCancel(includeQueued: boolean, includeRunning: boolean): ApiJobState[] {
  const result: ApiJobState[] = []
  if (includeQueued) {
    result.push(ApiJobState.Queued)
  }
  if (includeRunning) {
    result.push(ApiJobState.Pending, ApiJobState.Running)
  }
  return result
}

export default function CancelJobSetsDialog(props: CancelJobSetsDialogProps) {
  const [state, setState] = useState<CancelJobSetsDialogState>("CancelJobSets")
  const [response, setResponse] = useState<CancelJobSetsResponse>({
    cancelledJobSets: [],
    failedJobSetCancellations: [],
  })
  const [requestStatus, setRequestStatus] = useState<RequestStatus>("Idle")

  const [includeQueued, setIncludeQueued] = useState<boolean>(true)
  const [includeRunning, setIncludeRunning] = useState<boolean>(true)
  const [isPlatformCancel, setIsPlatformCancel] = useState(false)

  const jobSetsToCancel = getCancellableJobSets(props.selectedJobSets)

  const statesToCancel = getStatesToCancel(includeQueued, includeRunning)

  async function cancelJobSets() {
    if (requestStatus === "Loading") {
      return
    }

    setRequestStatus("Loading")
    const reason = isPlatformCancel ? PlatformCancelReason : ""
    const cancelJobSetsResponse = await props.updateJobSetsService.cancelJobSets(
      props.queue,
      jobSetsToCancel,
      statesToCancel,
      reason,
    )
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

  function cleanup() {
    setState("CancelJobSets")
    setResponse({
      cancelledJobSets: [],
      failedJobSetCancellations: [],
    })
    setIsPlatformCancel(false)
  }

  return (
    <Dialog
      open={props.isOpen}
      aria-labelledby="cancel-job-sets-modal-title"
      aria-describedby="cancel-job-sets-modal-description"
      onClose={props.onClose}
      TransitionProps={{
        onExited: cleanup,
      }}
      maxWidth={"md"}
    >
      <DialogTitle id="cancel-job-sets-dialog-title">Cancel Job Sets</DialogTitle>
      <DialogContent className="lookout-dialog">
        {state === "CancelJobSets" && (
          <CancelJobSets
            queue={props.queue}
            jobSets={jobSetsToCancel}
            queuedSelected={includeQueued}
            runningSelected={includeRunning}
            isLoading={requestStatus === "Loading"}
            onCancelJobSets={cancelJobSets}
            onQueuedSelectedChange={setIncludeQueued}
            onRunningSelectedChange={setIncludeRunning}
            isPlatformCancel={isPlatformCancel}
            setIsPlatformCancel={setIsPlatformCancel}
          />
        )}
        {state === "CancelJobSetsResult" && (
          <CancelJobSetsOutcome
            cancelJobSetsResponse={response}
            isLoading={requestStatus === "Loading"}
            queuedSelected={includeQueued}
            runningSelected={includeRunning}
            onCancelJobs={cancelJobSets}
            onQueuedSelectedChange={setIncludeQueued}
            onRunningSelectedChange={setIncludeRunning}
            isPlatformCancel={isPlatformCancel}
            setIsPlatformCancel={setIsPlatformCancel}
          />
        )}
      </DialogContent>
    </Dialog>
  )
}
