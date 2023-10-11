import React from "react"

import { Button, Checkbox, FormControl, FormControlLabel, InputLabel, Select, MenuItem } from "@material-ui/core"
import Alert from "@material-ui/lab/Alert"

import { getContainersForRun } from "../../containers/JobLogsContainer"
import { Job, UNKNOWN_CONTAINER } from "../../services/JobService"
import { LogLine } from "../../services/LogService"

import "./JobLogs.css"
import "../Dialog.css"

type JobLogsProps = {
  job: Job
  loadFromStart: boolean
  runIndex: number
  container: string
  log: LogLine[]
  error: string | undefined
  runError: string | undefined
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
          <FormControl>
            <InputLabel>Job Run</InputLabel>
            <Select
              value={props.runIndex}
              onChange={(e) => {
                const index = e.target.value as number
                props.onRunIndexChange(index)
                props.onContainerChange(getContainer(index, 0))
              }}
            >
              {props.job.runs?.map((run, i) => (
                <MenuItem value={i} key={i}>
                  {run.cluster} pod {run.podNumber} (start: {run.podStartTime})
                </MenuItem>
              ))}
            </Select>
          </FormControl>
        </div>
        <div className="job-logs-option">
          <FormControl>
            <InputLabel>Container</InputLabel>
            {
              <Select
                value={props.container}
                onChange={(e) => {
                  const container = e.target.value as string
                  props.onContainerChange(container)
                }}
              >
                {getContainersForRun(props.job, props.runIndex).map((container) => (
                  <MenuItem value={container} key={container}>
                    {container}
                  </MenuItem>
                ))}
              </Select>
            }
          </FormControl>
        </div>
        <div className="job-logs-option">
          <FormControl>
            <FormControlLabel
              className="no-label"
              label="Load from start"
              control={
                <Checkbox
                  checked={props.loadFromStart}
                  onChange={(e) => {
                    props.onLoadFromStartChange(e.target.checked)
                  }}
                />
              }
            />
          </FormControl>
        </div>
      </div>
      {!props.error && (
        <>
          <p className="lookout-dialog-varying job-logs">{props.log.map((l) => l.line).join("\n")}</p>
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
      {props.runError && (
        <p>
        <Alert className="lookout-dialog-centered lookout-dialog-fixed" severity="warning">
          {props.runError}
        </Alert>
        </p>
      )}
    </div>
  )
}
