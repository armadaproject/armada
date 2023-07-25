import React from "react"

import { Button, Checkbox, FormControl, FormControlLabel, InputLabel, Select, MenuItem } from "@material-ui/core"
import Alert from "@material-ui/lab/Alert"

import { getContainersForRun } from "../../containers/JobLogsContainer"
import { Job, UNKNOWN_CONTAINER } from "../../services/JobService"
import { LogLine } from "../../services/LogService"

import "./JobLogs.css"
import "../Dialog.css"
import JobLogsHeader from "./JobLogsHeader"
import JobLogsLoadMoreBtn from "./JobLogsLoadMoreBtn";



type JobLogsProps = {
  job: Job
  loadFromStart: boolean
  runIndex: number
  container: string
  log: LogLine[]
  error: string | undefined
  onLoadFromStartChange: (loadFromStart: boolean) => void
  onRunIndexChange: (runIndex: number) => void
  onContainerChange: (container: string) => void
  onLoadMoreClick: () => void
}

export default function JobLogs(props: JobLogsProps) {
  function getContainer(runIndex: number, containerIndex: number): string {
    const containers = getContainersForRun(props.job, runIndex)
    if (containerIndex >= containers.length) {
      return UNKNOWN_CONTAINER
    }
    return containers[containerIndex]
  }

  return (
    <div className="lookout-dialog-container">
      <div className="lookout-dialog-fixed job-logs-options">
        <div className="job-logs-option">
          {props.job.runs?.map((run, i) => (
              <section key={i} className="job-logs-option-group">
              <JobLogsHeader header='Cluster' headerValue={run?.cluster} />
              <JobLogsHeader header='Pod number' headerValue={run?.podNumber} />
              <JobLogsHeader header='Start time' headerValue= {run?.podStartTime} />
              </section>
              ))}
          {getContainersForRun(props.job, props.runIndex).map((container) => (
               <span key={container}>
               <JobLogsHeader header='Command' headerValue= {container} />
               </span>
            ))}
        </div>
        <JobLogsLoadMoreBtn text='Load from start'/>
      </div>
      {!props.error && (
        <>
          <p className="lookout-dialog-varying job-logs">
            {props.log.map((l) => <p key={l?.timestamp}>
                {l.line}
            </p>
            )}</p>
          <div className="lookout-dialog-centered lookout-dialog-fixed">
            <Button onClick={props.onLoadMoreClick}>Load more</Button>
          </div>
        </>
      )}
      {props.error && (
        <Alert className="lookout-dialog-centered lookout-dialog-fixed" severity="error">
          {props.error}
        </Alert>
      )}
    </div>
  )
}
