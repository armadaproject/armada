import React, { useEffect, useState } from "react"

import { Button, Checkbox, FormControl, FormControlLabel, InputLabel, Select, MenuItem } from "@material-ui/core"
import Alert from "@material-ui/lab/Alert"

import { Job } from "../../services/JobService"
import LogService, { LogLine } from "../../services/LogService"
import { getErrorMessage } from "../../utils"

import "./JobLogs.css"

export default function JobLogs(props: { logService: LogService; job: Job }) {
  const loadingLog = [{ text: "Loading ...", time: "" }]
  const runs = props.job.runs ?? []

  // local state
  const [logOptions, updateLogOptions] = useState({
    fromStart: false,
    runIndex: 0,
    container: runs[0].containers[0],
  })
  const [log, updateLog] = useState<LogLine[]>(loadingLog)
  const [error, setError] = useState<string | undefined>()

  const loadData = async (fromTime: string | undefined = undefined) => {
    setError(undefined)
    try {
      return await props.logService.getPodLogs(
        runs[logOptions.runIndex].cluster,
        props.job.jobId,
        props.job.namespace,
        runs[logOptions.runIndex].podNumber,
        logOptions.container,
        logOptions.fromStart || fromTime ? undefined : 100,
        fromTime,
      )
    } catch (e) {
      setError(await getErrorMessage(e))
      return []
    }
  }

  const firstLoad = async () => updateLog(await loadData())

  const loadMore = async () => {
    const lastLine = log[log.length - 1]
    const newLogData = await loadData(lastLine.time)
    // skip overlapping line
    if (newLogData.length > 0 && lastLine.text == newLogData[0].text && lastLine.time == newLogData[0].time) {
      newLogData.shift()
    }
    updateLog([...log, ...newLogData])
  }

  useEffect(() => firstLoad() && undefined, [props.job, logOptions])

  return (
    <div className="job-logs">
      <FormControl>
        <InputLabel>Job Run</InputLabel>
        <Select
          value={logOptions.runIndex}
          onChange={(e) =>
            updateLogOptions({
              ...logOptions,
              runIndex: e.target.value as number,
              container: runs[e.target.value as number].containers[0],
            })
          }
        >
          {props.job.runs?.map((r, i) => (
            <MenuItem value={i} key={i}>
              {r.cluster} pod {r.podNumber} (start: {r.podStartTime})
            </MenuItem>
          ))}
        </Select>
      </FormControl>
      <FormControl>
        <InputLabel>Container</InputLabel>
        <Select
          value={logOptions.container}
          onChange={(e) => updateLogOptions({ ...logOptions, container: e.target.value as string })}
        >
          {props.job.runs[logOptions.runIndex].containers.map((c) => (
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
          control={
            <Checkbox
              checked={logOptions.fromStart}
              onChange={(e) => updateLogOptions({ ...logOptions, fromStart: e.target.checked })}
            />
          }
        />
      </FormControl>
      {!error && (
        <>
          <pre className="log">{log.map((l) => l.text).join("\n")}</pre>
          <Button onClick={() => loadMore()}>Load more...</Button>
        </>
      )}
      {error && <Alert severity="error">{error}</Alert>}
    </div>
  )
}
