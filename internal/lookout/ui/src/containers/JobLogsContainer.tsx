import React, { useEffect, useState } from "react"

import JobLogs from "../components/job-dialog/JobLogs"
import { Job, UNKNOWN_CONTAINER } from "../services/JobService"
import LogService, { LogLine } from "../services/LogService"
import { getErrorMessage } from "../utils"

type JobLogsContainerProps = {
  job: Job
  logService: LogService
}

export function getContainersForRun(job: Job, runIndex: number): string[] {
  if (job.runs.length <= runIndex) {
    return []
  }
  const run = job.runs[runIndex]
  const containers = job.containers.get(run.podNumber)
  return containers ?? []
}

export default function JobLogsContainer(props: JobLogsContainerProps) {
  const loadingLog = [{ text: "Loading...", time: "" }]
  const runs = props.job.runs ?? []
  const defaultRunIndex = runs.length - 1
  const defaultRunContainers = getContainersForRun(props.job, defaultRunIndex)
  const defaultContainer = defaultRunContainers.length > 0 ? defaultRunContainers[0] : UNKNOWN_CONTAINER

  const [loadFromStart, setLoadFromStart] = useState<boolean>(false)
  const [runIndex, setRunIndex] = useState<number>(defaultRunIndex)
  const [container, setContainer] = useState<string>(defaultContainer)
  const [log, updateLog] = useState<LogLine[]>(loadingLog)
  const [error, setError] = useState<string | undefined>()

  async function loadData(fromTime: string | undefined = undefined): Promise<LogLine[]> {
    setError(undefined)
    try {
      return await props.logService.getPodLogs(
        runs[runIndex].cluster,
        props.job.jobId,
        props.job.namespace,
        runs[runIndex].podNumber,
        container,
        loadFromStart || fromTime ? undefined : 100,
        fromTime,
      )
    } catch (e) {
      setError(await getErrorMessage(e))
      return []
    }
  }

  async function firstLoad() {
    updateLog(await loadData())
  }

  async function loadMore() {
    const lastLine = log[log.length - 1]
    const newLogData = await loadData(lastLine.time)
    // skip overlapping line
    if (newLogData.length > 0 && lastLine.text == newLogData[0].text && lastLine.time == newLogData[0].time) {
      newLogData.shift()
    }
    updateLog([...log, ...newLogData])
  }

  useEffect(() => {
    firstLoad()
  }, [loadFromStart, runIndex, container])

  return (
    <JobLogs
      job={props.job}
      loadFromStart={loadFromStart}
      runIndex={runIndex}
      container={container}
      log={log}
      error={error}
      onLoadFromStartChange={setLoadFromStart}
      onRunIndexChange={setRunIndex}
      onContainerChange={setContainer}
      onLoadMoreClick={loadMore}
    />
  )
}
