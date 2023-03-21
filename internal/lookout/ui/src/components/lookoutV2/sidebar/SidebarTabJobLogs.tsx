import React, { useEffect, useMemo, useRef, useState } from "react"

import {
  Checkbox,
  CircularProgress,
  FormControl,
  FormControlLabel,
  FormGroup,
  InputLabel,
  MenuItem,
  Select,
} from "@mui/material"
import { Job } from "models/lookoutV2Models"

import { useCustomSnackbar } from "../../../hooks/useCustomSnackbar"
import { useJobSpec } from "../../../hooks/useJobSpec"
import { IGetJobSpecService } from "../../../services/lookoutV2/GetJobSpecService"
import { ILogService, LogLine } from "../../../services/lookoutV2/LogService"
import { getErrorMessage, RequestStatus } from "../../../utils"
import styles from "./SidebarTabJobLogs.module.css"

export interface SidebarTabJobLogsProps {
  job: Job
  jobSpecService: IGetJobSpecService
  logService: ILogService
}

function getContainers(jobSpec: Record<string, any> | undefined): string[] {
  if (jobSpec === undefined) {
    return []
  }
  let podSpec: Record<string, any> = {}
  if (jobSpec.podSpec !== undefined) {
    podSpec = jobSpec.podSpec
  }
  if (jobSpec.podSpecs !== undefined && Array.isArray(jobSpec.podSpecs) && jobSpec.podSpecs.length > 0) {
    podSpec = jobSpec.podSpecs[0]
  }

  const containers: string[] = []
  containers.push(...readContainerNames(podSpec.containers))
  containers.push(...readContainerNames(podSpec.initContainers))
  return containers
}

function readContainerNames(containers: any): string[] {
  if (containers === undefined || !Array.isArray(containers)) {
    return []
  }
  const containerNames: string[] = []
  for (const container of containers) {
    if (container.name !== undefined) {
      containerNames.push(container.name)
    }
  }
  return containerNames
}

const TAIL_LINES = 10
const TIMEOUT = 1000

export const SidebarTabJobLogs = ({ job, jobSpecService, logService }: SidebarTabJobLogsProps) => {
  const openSnackbar = useCustomSnackbar()
  const runsNewestFirst = useMemo(() => [...job.runs].reverse(), [job])
  const [runIndex, setRunIndex] = useState(0)
  const [selectedContainer, setSelectedContainer] = useState("")
  const [loadFromStart, setLoadFromStart] = useState(false)
  const [logs, setLogs] = useState<LogLine[]>([])
  const logsRef = useRef<LogLine[]>([]) // Cannot use state in setTimeout
  const [logsRequestStatus, setLogsRequestStatus] = useState<RequestStatus>("Idle")

  const jobSpecState = useJobSpec(job, jobSpecService, openSnackbar)

  function setLogsFull(newLogs: LogLine[]) {
    logsRef.current = newLogs
    setLogs(newLogs)
  }

  const containers = useMemo(() => getContainers(jobSpecState.jobSpec), [job, jobSpecState])
  const namespace = useMemo(() => {
    if (jobSpecState.jobSpec === undefined) {
      return ""
    }
    return jobSpecState.jobSpec.namespace ?? ""
  }, [job, jobSpecState])
  const cluster = useMemo(() => {
    if (job.runs.length === 0) {
      console.error("job has no runs")
      return ""
    }
    return job.runs[job.runs.length - 1].cluster
  }, [job])

  useEffect(() => {
    if (containers.length > 0) {
      setSelectedContainer(containers[0])
    }
  }, [containers])

  const loadLogs = async (sinceTime: string, tailLines: number | undefined): Promise<LogLine[]> => {
    setLogsRequestStatus("Loading")
    try {
      return await logService.getLogs(cluster, namespace, job.jobId, selectedContainer, sinceTime, tailLines, undefined)
    } catch (e) {
      const errMsg = await getErrorMessage(e)
      console.error(errMsg)
      openSnackbar("Failed to retrieve Job logs for Job with ID: " + job.jobId + ": " + errMsg, "error")
      return []
    } finally {
      setLogsRequestStatus("Idle")
    }
  }

  const mergeLogs = (newLogs: LogLine[]) => {
    const currentLogs = logsRef.current
    if (currentLogs.length === 0) {
      setLogsFull(newLogs)
      return
    }
    const lastLine = currentLogs[currentLogs.length - 1]
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
    setLogsFull([...currentLogs, ...newLogs.slice(indexToStartAppend)])
  }

  const loadFirst = async () => {
    let tailLines: number | undefined = TAIL_LINES
    if (loadFromStart) {
      tailLines = undefined
    }
    setLogsFull(await loadLogs("", tailLines))
  }

  const loadMore = async () => {
    const currentLogs = logsRef.current
    if (currentLogs.length === 0) {
      return loadFirst()
    }

    const lastLine = currentLogs[currentLogs.length - 1]
    const newLogs = await loadLogs(lastLine.timestamp, undefined)
    mergeLogs(newLogs)
  }

  const timeoutRef = useRef<NodeJS.Timeout | undefined>()
  const timerLoad = async () => {
    await loadMore()
    clearTimeout(timeoutRef.current)
    timeoutRef.current = setTimeout(async () => {
      await timerLoad()
    }, TIMEOUT)
  }

  useEffect(() => {
    if (selectedContainer === "") {
      return
    }

    loadFirst()
    timeoutRef.current = setTimeout(async () => {
      await timerLoad()
    }, TIMEOUT)

    return () => {
      clearTimeout(timeoutRef.current)
    }
  }, [job, runIndex, selectedContainer, loadFromStart])

  return (
    <div style={{ width: "100%", height: "100%" }}>
      {jobSpecState.loadState === "Loading" && (
        <div className={styles.loading}>
          <CircularProgress size={24} />
        </div>
      )}
      {jobSpecState.jobSpec && (
        <div
          style={{
            width: "100%",
            height: "100%",
            flexDirection: "column",
            display: "flex",
          }}
        >
          <div
            style={{
              display: "flex",
              flexDirection: "row",
              width: "100%",
              alignItems: "flex-start",
              justifyContent: "center",
            }}
          >
            <div
              style={{
                minWidth: "30%",
                flex: "1 1 auto",
                padding: "0 5px 0 5px",
              }}
            >
              <FormControl
                variant="standard"
                style={{
                  width: "100%",
                }}
              >
                <InputLabel id="select-job-run-label">Job Run</InputLabel>
                <Select
                  labelId="select-job-run-label"
                  variant="standard"
                  disabled={runsNewestFirst.length === 0}
                  value={runIndex}
                  size="small"
                  onChange={(e) => {
                    const index = e.target.value as number
                    setRunIndex(index)
                  }}
                  style={{
                    maxWidth: "300px",
                  }}
                >
                  {runsNewestFirst.map((run, i) => (
                    <MenuItem value={i} key={i}>
                      {run.started}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </div>
            <div
              style={{
                minWidth: "30%",
                flex: "1 1 auto",
                padding: "0 5px 0 5px",
              }}
            >
              <FormControl
                variant="standard"
                style={{
                  width: "100%",
                }}
              >
                <InputLabel id="select-container-label">Container</InputLabel>
                {
                  <Select
                    labelId="select-container-label"
                    variant="standard"
                    value={selectedContainer}
                    displayEmpty={true}
                    onChange={(e) => {
                      const container = e.target.value as string
                      setSelectedContainer(container)
                    }}
                    size="small"
                    style={{
                      maxWidth: "250px",
                    }}
                  >
                    {containers.map((container) => (
                      <MenuItem value={container} key={container}>
                        {container}
                      </MenuItem>
                    ))}
                  </Select>
                }
              </FormControl>
            </div>
            <div
              style={{
                minWidth: "30%",
                flex: "1 1 auto",
                padding: "0 5px 0 10px",
              }}
            >
              <FormGroup>
                <FormControlLabel
                  control={
                    <Checkbox
                      checked={loadFromStart}
                      onChange={(e) => {
                        setLoadFromStart(e.target.checked)
                      }}
                    />
                  }
                  label="Load from start"
                />
              </FormGroup>
            </div>
          </div>
          {logs.length > 0 ? (
            <div className={styles.logView}>
              {logs.map((logLine) => (
                <span>{logLine.line}</span>
              ))}
              {logsRequestStatus === "Loading" && (
                <div className={styles.loading}>
                  <CircularProgress size={24} />
                </div>
              )}
              <div className={styles.anchor} />
            </div>
          ) : (
            <div>No logs to display</div>
          )}
        </div>
      )}
    </div>
  )
}
