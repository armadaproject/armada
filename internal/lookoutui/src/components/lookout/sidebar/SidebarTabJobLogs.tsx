import { useEffect, useMemo, useRef, useState, UIEvent } from "react"

import { Refresh } from "@mui/icons-material"
import {
  Alert,
  alpha,
  Checkbox,
  FormControl,
  FormControlLabel,
  FormGroup,
  IconButton,
  InputLabel,
  MenuItem,
  Select,
  Skeleton,
  styled,
} from "@mui/material"

import { NoRunsAlert } from "./NoRunsAlert"
import { useCustomSnackbar } from "../../../hooks/useCustomSnackbar"
import { Job, JobRun } from "../../../models/lookoutModels"
import { LogLine } from "../../../services/lookout/LogService"
import { useGetJobSpec } from "../../../services/lookout/useGetJobSpec"
import { useGetLogs } from "../../../services/lookout/useGetLogs"
import { SPACING } from "../../../styling/spacing"

const LogsHeader = styled("div")(({ theme }) => ({
  maxWidth: "100%",
  display: "flex",
  flexDirection: "row",
  alignItems: "center",
  justifyContent: "start",
  gap: theme.spacing(SPACING.lg),
}))

const ContainerSelect = styled(Select)({
  width: "25ch",
})

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
    return getJobSpecResult.status === "success" ? (getJobSpecResult.data.namespace ?? "") : ""
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
    return <NoRunsAlert jobState={job.state} />
  }

  return (
    <>
      <LogsHeader>
        <div>
          <FormControl
            variant="standard"
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
            >
              {runsNewestFirst.map((run, i) => (
                <MenuItem value={i} key={i}>
                  {getJobRunTime(run)}
                </MenuItem>
              ))}
            </Select>
          </FormControl>
        </div>
        <div>
          <FormControl
            variant="standard"
            disabled={getJobSpecResult.status === "pending" || getLogsResult.status === "pending"}
          >
            <InputLabel id="select-container-label">Container</InputLabel>
            <ContainerSelect
              labelId="select-container-label"
              variant="standard"
              value={selectedContainer}
              displayEmpty={true}
              onChange={(e) => {
                const container = e.target.value as string
                setSelectedContainer(container)
              }}
              size="small"
            >
              {containers.map((container) => (
                <MenuItem value={container} key={container}>
                  {container}
                </MenuItem>
              ))}
            </ContainerSelect>
          </FormControl>
        </div>
        <div>
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
        <div>
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
      </LogsHeader>
      {getJobSpecResult.status === "pending" ||
        (getLogsResult.status === "pending" && (
          <LogsContainer>
            <Skeleton />
            <Skeleton />
            <Skeleton />
            <Skeleton />
            <Skeleton />
          </LogsContainer>
        ))}
      {getLogsResult.status === "success" && (
        <LogView logLines={getLogsResult.data?.pages?.flat() ?? []} showTimestamps={showTimestamps} />
      )}
      {getLogsResult.status === "error" && (
        <div>
          <Alert
            severity="error"
            action={
              <IconButton color="inherit" size="small" onClick={() => getLogsResult.refetch()}>
                <Refresh />
              </IconButton>
            }
          >
            <code>{getLogsResult.error}</code>
          </Alert>
        </div>
      )}
    </>
  )
}

const LogsContainer = styled("div")(({ theme }) => ({
  width: "100%",
  whiteSpace: "pre-wrap",
  fontFamily: "monospace",
  wordWrap: "break-word",
  marginTop: 5,
  backgroundColor: theme.palette.background.paper,
  padding: 5,
  borderRadius: 5,
  position: "relative",
  overflowY: "auto",
  overflowX: "auto",
}))

const Timestamp = styled("span")(({ theme }) => ({
  marginRight: 10,
  backgroundColor: alpha(theme.palette.primary.light, 0.15),
  ...theme.applyStyles("dark", {
    backgroundColor: alpha(theme.palette.primary.main, 0.15),
  }),
}))

function LogView({ logLines, showTimestamps }: { logLines: LogLine[]; showTimestamps: boolean }) {
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

  if (logLines.length === 0) {
    return (
      <Alert variant="outlined" severity="info">
        No logs to display
      </Alert>
    )
  }

  return (
    <LogsContainer onScroll={handleScroll}>
      {logLines.map((logLine, i) => (
        <span key={`${i}-${logLine.timestamp}`}>
          {showTimestamps && <Timestamp>{logLine.timestamp}</Timestamp>}
          {logLine.line + "\n"}
        </span>
      ))}
      <div ref={logsEndRef} key={"END"} />
    </LogsContainer>
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
