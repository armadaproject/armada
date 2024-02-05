import React, { useEffect, useMemo, useRef, useState, UIEvent } from "react"

import { Refresh } from "@mui/icons-material"
import {
  Checkbox,
  CircularProgress,
  FormControl,
  FormControlLabel,
  FormGroup,
  IconButton,
  InputLabel,
  MenuItem,
  Select,
} from "@mui/material"
import { Job, JobRun } from "models/lookoutV2Models"

import styles from "./SidebarTabJobLogs.module.css"
import { useCustomSnackbar } from "../../../hooks/useCustomSnackbar"
import { useJobSpec } from "../../../hooks/useJobSpec"
import { getAccessToken, useUserManager } from "../../../oidc"
import { IGetJobSpecService } from "../../../services/lookoutV2/GetJobSpecService"
import { ILogService, LogLine } from "../../../services/lookoutV2/LogService"
import { getErrorMessage, RequestStatus } from "../../../utils"

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

const TAIL_LINES = 1000
const TIMEOUT = 1000

export const SidebarTabJobLogs = ({ job, jobSpecService, logService }: SidebarTabJobLogsProps) => {
  const openSnackbar = useCustomSnackbar()
  const runsNewestFirst = useMemo(() => [...job.runs].reverse(), [job])
  const [runIndex, setRunIndex] = useState(0)
  const [selectedContainer, setSelectedContainer] = useState("")
  const [loadFromStart, setLoadFromStart] = useState(false)
  const [showTimestamps, setShowTimestamps] = useState(false)
  const [logs, setLogs] = useState<LogLine[]>([])
  const logsRef = useRef<LogLine[]>([]) // Cannot use state in setTimeout
  const [logsRequestStatus, setLogsRequestStatus] = useState<RequestStatus>("Idle")
  const [logsRequestError, setLogsRequestError] = useState<string | undefined>(undefined)
  const logsRequestErrorRef = useRef<string | undefined>(undefined)

  const jobSpecState = useJobSpec(job, jobSpecService, openSnackbar)

  const setLogsFull = (newLogs: LogLine[]) => {
    logsRef.current = newLogs
    setLogs(newLogs)
  }

  const setLogsRequestErrorFull = (error: string | undefined) => {
    logsRequestErrorRef.current = error
    setLogsRequestError(error)
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

  const userManager = useUserManager()

  const loadLogs = async (sinceTime: string, tailLines: number | undefined): Promise<LogLine[]> => {
    setLogsRequestStatus("Loading")
    try {
      const accessToken = userManager && (await getAccessToken(userManager))
      const logLines = await logService.getLogs(
        cluster,
        namespace,
        job.jobId,
        selectedContainer,
        sinceTime,
        tailLines,
        accessToken,
      )
      setLogsRequestErrorFull(undefined)
      return logLines
    } catch (e) {
      const errMsg = await getErrorMessage(e)
      setLogsRequestErrorFull(errMsg)
      console.error(errMsg)
      openSnackbar("Failed to retrieve Job logs for Job with ID: " + job.jobId + ": " + errMsg, "error", {
        autoHideDuration: 5000,
      })
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

  const timeoutRef = useRef<NodeJS.Timeout | undefined>(undefined)
  const timerLoad = async () => {
    if (logsRequestErrorRef.current !== undefined) {
      return
    }
    await loadMore()
    clearTimeout(timeoutRef.current)
    timeoutRef.current = setTimeout(async () => {
      await timerLoad()
    }, TIMEOUT)
  }

  const refresh = async () => {
    await loadFirst()
    clearTimeout(timeoutRef.current)
    timeoutRef.current = setTimeout(async () => {
      await timerLoad()
    }, TIMEOUT)
  }

  useEffect(() => {
    if (selectedContainer === "" || job.runs.length === 0) {
      return
    }

    refresh()

    return () => {
      setLogsRequestErrorFull(undefined)
      clearTimeout(timeoutRef.current)
    }
  }, [job, runIndex, selectedContainer, loadFromStart])

  if (job.runs.length === 0) {
    return <div className={styles.didNotRun}>This job did not run.</div>
  }

  if (jobSpecState.jobSpec === undefined) {
    return (
      <div className={styles.loading}>
        <CircularProgress size={24} />
      </div>
    )
  }

  return (
    <div className={styles.sidebarLogsTabContainer}>
      {jobSpecState.loadState === "Loading" && (
        <div className={styles.loading}>
          <CircularProgress size={24} />
        </div>
      )}
      <div className={styles.logsHeader}>
        <div className={styles.logOption}>
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
                  {getJobRunTime(run)}
                </MenuItem>
              ))}
            </Select>
          </FormControl>
        </div>
        <div className={styles.logOption}>
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
        <div className={styles.logOption}>
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
        <div className={styles.logOption}>
          <FormGroup>
            <FormControlLabel
              control={
                <Checkbox
                  checked={showTimestamps}
                  onChange={(e) => {
                    setShowTimestamps(e.target.checked)
                  }}
                />
              }
              label="Show timestamps"
            />
          </FormGroup>
        </div>
      </div>
      <LogView logLines={logs} showTimestamps={showTimestamps} />
      <div className={styles.gutter}>
        {logsRequestStatus === "Loading" && (
          <div className={styles.loading}>
            <CircularProgress size={24} />
          </div>
        )}
        {logsRequestError !== undefined && (
          <>
            <div className={styles.errorMessage}>{logsRequestError}</div>
            <div>
              <IconButton onClick={refresh}>
                <Refresh />
              </IconButton>
            </div>
          </>
        )}
      </div>
    </div>
  )
}

function LogView({ logLines, showTimestamps }: { logLines: LogLine[]; showTimestamps: boolean }) {
  if (logLines.length === 0) {
    return (
      <div key={"EMPTY"} className={styles.emptyLogView}>
        No logs to display
      </div>
    )
  }

  const [shouldScrollDown, setShouldScrollDown] = useState<boolean>(true)
  const logsEndRef = useRef<HTMLDivElement>(null)
  const previousScrollTopRef = useRef<number | undefined>()

  const scrollToBottom = () => {
    if (shouldScrollDown) {
      logsEndRef.current?.scrollIntoView({ behavior: "smooth" })
    }
  }

  useEffect(() => {
    scrollToBottom()
  }, [logLines])

  const handleScroll = (e: UIEvent<HTMLDivElement>) => {
    const element = e.currentTarget
    const scrollHeight = element.scrollHeight
    const scrollTop = element.scrollTop
    const clientHeight = element.clientHeight

    const previousScrollTop = previousScrollTopRef.current
    if (previousScrollTop && previousScrollTop >= scrollTop) {
      setShouldScrollDown(false)
    }
    previousScrollTopRef.current = scrollTop

    const isAtBottom = Math.round(scrollHeight - scrollTop) === clientHeight
    if (isAtBottom) {
      setShouldScrollDown(true)
    }
  }

  return (
    <div className={styles.logView} onScroll={handleScroll}>
      {logLines.map((logLine, i) => (
        <span key={`${i}-${logLine.timestamp}`}>
          {showTimestamps && <span className={styles.timestamp}>{logLine.timestamp}</span>}
          {logLine.line + "\n"}
        </span>
      ))}
      <div ref={logsEndRef} key={"END"} />
    </div>
  )
}

function getJobRunTime(run: JobRun): string {
  if (run.started !== undefined && run.started !== "") {
    return run.started
  }
  if (run.pending !== undefined && run.pending !== "") {
    return run.pending
  }
  if (run.leased !== undefined && run.leased !== "") {
    return run.leased
  }
  return ""
}
