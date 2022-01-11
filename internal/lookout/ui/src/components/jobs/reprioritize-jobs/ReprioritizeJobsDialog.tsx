import React, { useState } from "react"

import { Dialog, DialogContent, DialogTitle } from "@material-ui/core"

import { REPRIORITIZEABLE_JOB_STATES } from "../../../containers/JobsContainer"
import JobService, { Job, ReprioritizeJobsResult } from "../../../services/JobService"
import { RequestStatus } from "../../../utils"
import ReprioritizeJobs from "./ReprioritizeJobs"
import ReprioritizeJobsOutcome from "./ReprioritizeJobsOutcome"

export type ReprioritizeJobsDialogState = "ReprioritizeJobs" | "ReprioritizeJobsResult"
export type ReprioritizeJobsStatus = "Success" | "Failure" | "Partial success"

type ReprioritizeJobsProps = {
  isOpen: boolean
  selectedJobs: Job[]
  jobService: JobService
  onResult(result: ReprioritizeJobsStatus): void
  onClose(): void
}

const newPriorityRegex = new RegExp("^([0-9]+)$")

function priorityIsValid(priority: string): boolean {
  return newPriorityRegex.test(priority) && priority.length > 0
}

export default function ReprioritizeJobsDialog(props: ReprioritizeJobsProps) {
  const [state, setState] = useState<ReprioritizeJobsDialogState>("ReprioritizeJobs")
  const [result, setResult] = useState<ReprioritizeJobsResult>({
    reprioritizedJobs: [],
    failedJobReprioritizations: [],
  })
  const [requestStatus, setRequestStatus] = useState<RequestStatus>("Idle")
  const [priority, setPriority] = useState<string>("")

  const jobsToReprioritize = props.selectedJobs.filter((job) => REPRIORITIZEABLE_JOB_STATES.includes(job.jobState))

  async function reprioritizeJobs() {
    if (requestStatus == "Loading" || !priorityIsValid(priority)) {
      return
    }

    setRequestStatus("Loading")
    const reprioritizeJobsResult = await props.jobService.reprioritizeJobs(jobsToReprioritize, Number(priority))
    setRequestStatus("Idle")

    setResult(reprioritizeJobsResult)
    setState("ReprioritizeJobsResult")
    if (reprioritizeJobsResult.failedJobReprioritizations.length === 0) {
      props.onResult("Success")
    } else if (reprioritizeJobsResult.reprioritizedJobs.length === 0) {
      props.onResult("Failure")
    } else {
      props.onResult("Partial success")
    }
  }

  function close() {
    setState("ReprioritizeJobs")
    setResult({
      reprioritizedJobs: [],
      failedJobReprioritizations: [],
    })
    props.onClose()
  }

  let content: JSX.Element
  switch (state) {
    case "ReprioritizeJobs":
      content = (
        <ReprioritizeJobs
          jobsToReprioritize={jobsToReprioritize}
          isLoading={requestStatus == "Loading"}
          newPriority={priority}
          isValid={priorityIsValid(priority)}
          onReprioritizeJobs={reprioritizeJobs}
          onPriorityChange={setPriority}
        />
      )
      break
    case "ReprioritizeJobsResult":
      content = (
        <ReprioritizeJobsOutcome
          reprioritizeJobsResult={result}
          isLoading={requestStatus == "Loading"}
          newPriority={priority}
          onReprioritizeJobs={reprioritizeJobs}
        />
      )
      break
    default:
      content = <div />
  }

  return (
    <Dialog
      open={props.isOpen}
      aria-labelledby="reprioritize-jobs-modal-title"
      aria-describedby="reprioritize-jobs-modal-description"
      onClose={close}
      maxWidth={"md"}
    >
      <DialogTitle id="reprioritize-jobs-dialog-title">Reprioritize Jobs</DialogTitle>
      <DialogContent>{content}</DialogContent>
    </Dialog>
  )
}
