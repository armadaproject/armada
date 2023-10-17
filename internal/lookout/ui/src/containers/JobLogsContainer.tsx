import React, { useEffect, useState } from "react"

import JobLogs from "../components/job-dialog/JobLogs"
import { Job, UNKNOWN_CONTAINER } from "../services/JobService"
import LogService, { LogLine } from "../services/LogService"
import { getErrorMessage } from "../utils"

type JobLogsContainerProps = {
  job: Job
  logService: LogService
}

const TAIL_LINES = 100

export function getContainersForRun(job: Job, runIndex: number): string[] {
  if (job.runs.length <= runIndex) {
    return []
  }
  const run = job.runs[runIndex]
  const containers = job.containers.get(run.podNumber)
  return containers ?? []
}

export default function JobLogsContainer(props: JobLogsContainerProps) {
  const loadingLog = [{ line: "Loading...", timestamp: "" }]
  const runs = props.job.runs ?? []
  const defaultRunIndex = runs.length - 1
  const defaultRunContainers = getContainersForRun(props.job, defaultRunIndex)
  const defaultContainer = defaultRunContainers.length > 0 ? defaultRunContainers[0] : UNKNOWN_CONTAINER

  const [loadFromStart, setLoadFromStart] = useState<boolean>(false)
  const [runIndex, setRunIndex] = useState<number>(defaultRunIndex)
  const [container, setContainer] = useState<string>(defaultContainer)
  const [log, updateLog] = useState<LogLine[]>(loadingLog)
  const [error, setError] = useState<string | undefined>()
  const runError = props.job.runs[defaultRunIndex].error || undefined

  async function loadLogs(sinceTime: string, tailLines: number | undefined): Promise<LogLine[]> {
    setError(undefined)
    try {
      return await props.logService.getPodLogs({
        clusterId: runs[runIndex].cluster,
        namespace: props.job.namespace,
        jobId: props.job.jobId,
        podNumber: runs[runIndex].podNumber,
        container: container,
        sinceTime: sinceTime,
        tailLines: tailLines,
      })
    } catch (e) {
      setError(await getErrorMessage(e))
      return []
    }
  }

  async function loadFirst() {
    let tailLines: number | undefined = TAIL_LINES
    if (loadFromStart) {
      tailLines = undefined
    }
    updateLog(await loadLogs("", tailLines))
  }

  async function loadMore() {
    if (log.length === 0) {
      return loadFirst()
    }

    const lastLine = log[log.length - 1]
    const newLogs = await loadLogs(lastLine.timestamp, undefined)
    mergeLogs(newLogs)
  }

  function mergeLogs(newLogs: LogLine[]) {
    const lastLine = log[log.length - 1]
    let indexToStartAppend = 0
    for (let i = 0; i < newLogs.length; i++) {
      if (newLogs[i].timestamp > lastLine.timestamp) {
        break
      }
      indexToStartAppend += 1
    }

    if (indexToStartAppend >= newLogs.length) {
      return
    }

    updateLog([...log, ...newLogs.slice(indexToStartAppend)])
  }

  useEffect(() => {
    loadFirst()
  }, [loadFromStart, runIndex, container])

  return (
    <JobLogs
      job={props.job}
      loadFromStart={loadFromStart}
      runIndex={runIndex}
      container={container}
      log={log}
      error={error}
      runError={runError}
      onLoadFromStartChange={setLoadFromStart}
      onRunIndexChange={setRunIndex}
      onContainerChange={setContainer}
      onLoadMoreClick={loadMore}
    />
  )
}
