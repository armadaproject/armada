import { useEffect, useMemo, useRef, useState, UIEvent } from "react"

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
import { LogLine } from "../../../services/lookoutV2/LogService"
import { useGetJobSpec } from "../../../services/lookoutV2/useGetJobSpec"
import { useGetLogs } from "../../../services/lookoutV2/useGetLogs"

export interface SidebarTabJobLogsProps {
  job: Job
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

const INTERVAL_MS = 1_000

export const SidebarTabJobLogs = ({ job }: SidebarTabJobLogsProps) => {
  const openSnackbar = useCustomSnackbar()
  const runsNewestFirst = useMemo(() => [...job.runs].reverse(), [job])
  const [runIndex, setRunIndex] = useState(0)
  const [selectedContainer, setSelectedContainer] = useState("")
  const [loadFromStart, setLoadFromStart] = useState(false)
  const [showTimestamps, setShowTimestamps] = useState(false)

  // Get job spec
  const getJobSpecResult = useGetJobSpec(job.jobId, Boolean(job.jobId))
  useEffect(() => {
    if (getJobSpecResult.status === "error") {
      openSnackbar(`Failed to retrieve Job spec for Job with ID: ${job.jobId}: ${getJobSpecResult.error}`, "error")
    }
  }, [getJobSpecResult.status, getJobSpecResult.error])
  const containers = useMemo(
    () => (getJobSpecResult.status === "success" ? getContainers(getJobSpecResult.data) : []),
    [getJobSpecResult.status, getJobSpecResult.data],
  )
  useEffect(() => {
    setSelectedContainer(containers?.[0] ?? "")
  }, [containers])

  const namespace = useMemo(() => {
    return getJobSpecResult.status === "success" ? getJobSpecResult.data.namespace ?? "" : ""
  }, [getJobSpecResult.status, getJobSpecResult.data])

  const cluster = useMemo(() => {
    if (job.runs.length === 0) {
      console.error("job has no runs")
      return ""
    }
    return job.runs[job.runs.length - 1].cluster
  }, [job])

  // Get logs
  const getLogsEnabled = Boolean(cluster && namespace && job.jobId && selectedContainer && job.runs.length > 0)
  const getLogsResult = useGetLogs(cluster, namespace, job.jobId, selectedContainer, loadFromStart, getLogsEnabled)
  useEffect(() => {
    if (getLogsResult.status === "error") {
      openSnackbar(`Failed to retrieve Job logs for Job with ID:  ${job.jobId}: ${getLogsResult.error}`, "error", {
        autoHideDuration: 5000,
      })
    }
  }, [getLogsResult.status, getLogsResult.error])

  // Periodically refetch logs
  useEffect(() => {
    const interval = setInterval(() => {
      if (getLogsResult.status == "success" && getLogsEnabled) {
        getLogsResult.fetchNextPage()
      }
    }, INTERVAL_MS)

    return () => {
      clearInterval(interval)
    }
  }, [getLogsResult.fetchNextPage, getLogsResult.status, getLogsEnabled])

  if (job.runs.length === 0) {
    return <div className={styles.didNotRun}>This job did not run.</div>
  }

  return (
    <div className={styles.sidebarLogsTabContainer}>
      <div className={styles.logsHeader}>
        <div className={styles.logOption}>
          <FormControl
            variant="standard"
            style={{
              width: "100%",
            }}
            disabled={
              getJobSpecResult.status === "pending" ||
              getLogsResult.status === "pending" ||
              runsNewestFirst.length === 0
            }
          >
            <InputLabel id="select-job-run-label">Job Run</InputLabel>
            <Select
              labelId="select-job-run-label"
              variant="standard"
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
            disabled={getJobSpecResult.status === "pending" || getLogsResult.status === "pending"}
          >
            <InputLabel id="select-container-label">Container</InputLabel>
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
              disabled={getJobSpecResult.status === "pending" || getLogsResult.status === "pending"}
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
              disabled={getJobSpecResult.status === "pending" || getLogsResult.status === "pending"}
            />
          </FormGroup>
        </div>
      </div>
      {getJobSpecResult.status === "pending" ||
        (getLogsResult.status === "pending" && (
          <div className={styles.loading}>
            <CircularProgress size={24} />
          </div>
        ))}
      {getLogsResult.status === "success" && (
        <LogView logLines={getLogsResult.data?.pages?.flat() ?? []} showTimestamps={showTimestamps} />
      )}
      <div className={styles.gutter}>
        {getLogsResult.status === "error" && (
          <>
            <div className={styles.errorMessage}>{getLogsResult.error}</div>
            <div>
              <IconButton onClick={() => getLogsResult.refetch()}>
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
