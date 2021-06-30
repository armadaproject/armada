import React, { useEffect, useState } from "react"

import { Button, Checkbox, FormControl, FormControlLabel, InputLabel, Select, MenuItem } from "@material-ui/core"

import { Job } from "../../services/JobService"
import LogService, { LogLine } from "../../services/LogService"

import "./JobLogs.css"

export default function JobLogs(props: { logService: LogService; job: Job }) {
  const loadingLog = [{ text: "Loading ...", time: "" }]
  const runs = props.job.runs ?? []

  // local state
  const [log, updateLog] = useState<LogLine[]>(loadingLog)
  const [fromStart, updateFromStart] = useState(false)
  const [runIndex, updateRunIndex] = useState(0)
  const [container, updateContainer] = useState(runs[0].containers[0])

  const loadData = async (fromTime: string | undefined = undefined) =>
    props.logService.getPodLogs(
      runs[runIndex].cluster,
      props.job.jobId,
      props.job.namespace,
      runs[runIndex].podNumber,
      container,
      fromStart || fromTime ? undefined : 100,
      fromTime,
    )

  const firstLoad = async () => {
    if (!runs[runIndex].containers.includes(container)) {
      updateContainer(runs[runIndex].containers[0])
      return // update will trigger another reload
    }
    updateLog(await loadData())
  }

  const loadMore = async () => {
    const lastLine = log[log.length - 1]
    const newLogData = await loadData(lastLine.time)
    // skip overlapping line
    if (newLogData.length > 0 && lastLine.text == newLogData[0].text && lastLine.time == newLogData[0].time) {
      newLogData.shift()
    }
    updateLog([...log, ...newLogData])
  }

  useEffect(() => firstLoad() && undefined, [props.job, fromStart, runIndex, container])

  return (
    <div className="job-logs">
      <FormControl>
        <InputLabel>Job Run</InputLabel>
        <Select value={runIndex} onChange={(e) => updateRunIndex(e.target.value as number)}>
          {props.job.runs?.map((r, i) => (
            <MenuItem value={i} key={i}>
              {r.cluster} pod {r.podNumber} (start: {r.podStartTime})
            </MenuItem>
          ))}
        </Select>
      </FormControl>
      <FormControl>
        <InputLabel>Container</InputLabel>
        <Select value={container} onChange={(e) => updateContainer(e.target.value as string)}>
          {props.job.runs[runIndex].containers.map((c) => (
            <MenuItem value={c} key={c}>
              {c}
            </MenuItem>
          ))}
        </Select>
      </FormControl>
      <FormControl>
        <FormControlLabel
          className="no-label"
          label="Load from start"
          control={<Checkbox checked={fromStart} onChange={(e) => updateFromStart(e.target.checked)} />}
        />
      </FormControl>
      <pre className="log">{log.map((l) => l.text).join("\n")}</pre>
      <Button onClick={() => loadMore()}>Load more...</Button>
    </div>
  )
}
