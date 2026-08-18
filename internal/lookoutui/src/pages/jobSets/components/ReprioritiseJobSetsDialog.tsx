import { useState } from "react"

import { Dialog, DialogContent, DialogTitle } from "@mui/material"
import { ErrorBoundary } from "react-error-boundary"

import { ApiResult, getErrorMessage, priorityIsValid, RequestStatus } from "../../../common/utils"
import { AlertErrorFallback } from "../../../components/AlertErrorFallback"
import { useCustomSnackbar } from "../../../components/hooks/useCustomSnackbar"
import { JobSet } from "../../../models/lookoutModels"
import { ReprioritiseJobSetsResponse, useReprioritiseJobSets } from "../../../services/lookout/useReprioritiseJobSets"

import ReprioritiseJobSets from "./reprioritise-job-sets/ReprioritiseJobSets"
import ReprioritiseJobSetsOutcome from "./reprioritise-job-sets/ReprioritiseJobSetsOutcome"

import "./Dialog.css"

export type ReprioritiseJobSetsDialogState = "ReprioritiseJobSets" | "ReprioritiseJobSetsResult"

type ReprioritiseJobSetsDialogProps = {
  isOpen: boolean
  queue: string
  selectedJobSets: JobSet[]
  onResult: (result: ApiResult) => void
  onClose: () => void
}

export function getReprioritisableJobSets(jobSets: JobSet[]): JobSet[] {
  return jobSets.filter((jobSet) => jobSet.jobsQueued > 0)
}

export default function ReprioritiseJobSetsDialog(props: ReprioritiseJobSetsDialogProps) {
  const [state, setState] = useState<ReprioritiseJobSetsDialogState>("ReprioritiseJobSets")
  const [response, setResponse] = useState<ReprioritiseJobSetsResponse>({
    reprioritisedJobSets: [],
    failedJobSetReprioritisations: [],
  })
  const [requestStatus, setRequestStatus] = useState<RequestStatus>("Idle")
  const [priority, setPriority] = useState<string>("")

  const jobSetsToReprioritise = getReprioritisableJobSets(props.selectedJobSets)

  const reprioritiseJobSetsMutation = useReprioritiseJobSets()
  const openSnackbar = useCustomSnackbar()

  async function reprioritiseJobSets() {
    if (requestStatus == "Loading" || !priorityIsValid(priority)) {
      return
    }

    setRequestStatus("Loading")
    try {
      const reprioritiseJobSetsResponse = await reprioritiseJobSetsMutation.mutateAsync({
        queue: props.queue,
        jobSets: jobSetsToReprioritise,
        newPriority: Number(priority),
      })

      setResponse(reprioritiseJobSetsResponse)
      setState("ReprioritiseJobSetsResult")
      if (reprioritiseJobSetsResponse.failedJobSetReprioritisations.length === 0) {
        props.onResult("Success")
      } else if (reprioritiseJobSetsResponse.reprioritisedJobSets.length === 0) {
        props.onResult("Failure")
      } else {
        props.onResult("Partial success")
      }
    } catch (e) {
      openSnackbar(`Failed to reprioritise job sets: ${await getErrorMessage(e)}`, "error")
    } finally {
      setRequestStatus("Idle")
    }
  }

  function cleanup() {
    setPriority("")
    setState("ReprioritiseJobSets")
    setResponse({
      reprioritisedJobSets: [],
      failedJobSetReprioritisations: [],
    })
  }

  return (
    <Dialog
      open={props.isOpen}
      aria-labelledby="reprioritise-job-sets-dialog-title"
      aria-describedby="reprioritise-job-sets-dialog-description"
      onClose={props.onClose}
      TransitionProps={{
        onExited: cleanup,
      }}
      maxWidth={"md"}
    >
      <DialogTitle id="-reprioritise-job-sets-dialog-title">Reprioritise Job Sets</DialogTitle>
      <DialogContent className="lookout-dialog">
        <ErrorBoundary FallbackComponent={AlertErrorFallback}>
          {state === "ReprioritiseJobSets" && (
            <ReprioritiseJobSets
              queue={props.queue}
              jobSets={jobSetsToReprioritise}
              isLoading={requestStatus === "Loading"}
              isValid={priorityIsValid(priority)}
              onReprioritiseJobsSets={reprioritiseJobSets}
              onPriorityChange={setPriority}
            />
          )}
          {state === "ReprioritiseJobSetsResult" && (
            <ReprioritiseJobSetsOutcome
              reprioritiseJobSetResponse={response}
              isLoading={requestStatus === "Loading"}
              newPriority={priority}
              onReprioritiseJobSets={reprioritiseJobSets}
            />
          )}
        </ErrorBoundary>
      </DialogContent>
    </Dialog>
  )
}
