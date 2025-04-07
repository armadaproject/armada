import { useCallback, useEffect, useMemo, useRef, useState } from "react"

import { ExpandMore } from "@mui/icons-material"
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Skeleton,
  styled,
  Tooltip,
  Typography,
} from "@mui/material"

import { KeyValuePairTable } from "./KeyValuePairTable"
import { NoRunsAlert } from "./NoRunsAlert"
import { SidebarTabHeading, SidebarTabSubheading } from "./sidebarTabContentComponents"
import { formatDuration } from "../../../common/formatTime"
import { useFormatIsoTimestampWithUserSettings } from "../../../hooks/formatTimeWithUserSettings"
import { useCustomSnackbar } from "../../../hooks/useCustomSnackbar"
import { Job, JobState } from "../../../models/lookoutModels"
import { useGetAccessToken } from "../../../oidcAuth"
import { ICordonService } from "../../../services/lookout/CordonService"
import { IGetJobInfoService } from "../../../services/lookout/GetJobInfoService"
import { IGetRunInfoService } from "../../../services/lookout/GetRunInfoService"
import { SPACING } from "../../../styling/spacing"
import { getErrorMessage } from "../../../utils"
import { formatJobRunState } from "../../../utils/jobsTableFormatters"
import { CodeBlock } from "../../CodeBlock"
import { ErrorBoundary } from "react-error-boundary"
import { AlertErrorFallback } from "../../AlertErrorFallback"

const MarkNodeUnschedulableButtonContainer = styled("div")(({ theme }) => ({
  display: "flex",
  justifyContent: "center",
  padding: theme.spacing(SPACING.sm),
}))

const loadingAccordion = (
  <Accordion>
    <AccordionDetails>
      <Skeleton />
    </AccordionDetails>
  </Accordion>
)

export interface SidebarTabJobResultProps {
  job: Job
  runInfoService: IGetRunInfoService
  jobInfoService: IGetJobInfoService
  cordonService: ICordonService
}

type LoadState = "Idle" | "Loading"

export const SidebarTabJobResult = ({
  job,
  jobInfoService,
  runInfoService,
  cordonService,
}: SidebarTabJobResultProps) => {
  const mounted = useRef(false)
  const openSnackbar = useCustomSnackbar()

  const formatIsoTimestamp = useFormatIsoTimestampWithUserSettings()

  const runsNewestFirst = useMemo(() => [...job.runs].reverse(), [job])
  const [jobError, setJobError] = useState<string>("")
  const [runErrorMap, setRunErrorMap] = useState<Map<string, string>>(new Map<string, string>())
  const [runErrorLoadingMap, setRunErrorLoadingMap] = useState<Map<string, LoadState>>(new Map<string, LoadState>())
  const [runDebugMessageMap, setRunDebugMessageMap] = useState<Map<string, string>>(new Map<string, string>())
  const [runDebugMessageLoadingMap, setRunDebugMessageLoadingMap] = useState<Map<string, LoadState>>(
    new Map<string, LoadState>(),
  )
  const [open, setOpen] = useState(false)

  const fetchJobError = useCallback(async () => {
    if (job.state != JobState.Failed && job.state != JobState.Rejected) {
      setJobError("")
      return
    }
    const getJobErrorResultPromise = jobInfoService.getJobError(job.jobId)
    getJobErrorResultPromise
      .then((errorString) => {
        if (!mounted.current) {
          return
        }
        setJobError(errorString)
      })
      .catch(async (e) => {
        const errMsg = await getErrorMessage(e)
        console.error(errMsg)
        if (!mounted.current) {
          return
        }
        openSnackbar("Failed to retrieve Job error for Job with ID: " + job.jobId + ": " + errMsg, "error")
      })
  }, [job])

  const fetchRunErrors = useCallback(async () => {
    const newRunErrorLoadingMap = new Map<string, LoadState>()
    for (const run of job.runs) {
      newRunErrorLoadingMap.set(run.runId, "Loading")
    }
    setRunErrorLoadingMap(newRunErrorLoadingMap)

    const results: { runId: string; promise: Promise<string> }[] = []
    for (const run of job.runs) {
      results.push({
        runId: run.runId,
        promise: runInfoService.getRunError(run.runId),
      })
    }

    const newRunErrorMap = new Map<string, string>(runErrorMap)
    for (const result of results) {
      result.promise
        .then((errorString) => {
          if (!mounted.current) {
            return
          }
          newRunErrorMap.set(result.runId, errorString)
          setRunErrorMap(new Map(newRunErrorMap))
        })
        .catch(async (e) => {
          const errMsg = await getErrorMessage(e)
          console.error(errMsg)
          if (!mounted.current) {
            return
          }
          openSnackbar("Failed to retrieve Job Run error for Run with ID: " + result.runId + ": " + errMsg, "error")
        })
        .finally(() => {
          if (!mounted.current) {
            return
          }
          newRunErrorLoadingMap.set(result.runId, "Idle")
          setRunErrorLoadingMap(new Map(newRunErrorLoadingMap))
        })
    }
  }, [job])

  const fetchRunDebugMessages = useCallback(async () => {
    const newRunDebugMessageLoadingMap = new Map<string, LoadState>()
    for (const run of job.runs) {
      newRunDebugMessageLoadingMap.set(run.runId, "Loading")
    }
    setRunDebugMessageLoadingMap(newRunDebugMessageLoadingMap)

    const results: { runId: string; promise: Promise<string> }[] = []
    for (const run of job.runs) {
      results.push({
        runId: run.runId,
        promise: runInfoService.getRunDebugMessage(run.runId),
      })
    }

    const newRunDebugMessageMap = new Map<string, string>(runErrorMap)
    for (const result of results) {
      result.promise
        .then((debugMessage) => {
          if (!mounted.current) {
            return
          }
          newRunDebugMessageMap.set(result.runId, debugMessage)
          setRunDebugMessageMap(new Map(newRunDebugMessageMap))
        })
        .catch(async (e) => {
          const errMsg = await getErrorMessage(e)
          console.error(errMsg)
          if (!mounted.current) {
            return
          }
          openSnackbar(
            "Failed to retrieve Job Run debug message for Run with ID: " + result.runId + ": " + errMsg,
            "error",
          )
        })
        .finally(() => {
          if (!mounted.current) {
            return
          }
          newRunDebugMessageLoadingMap.set(result.runId, "Idle")
          setRunErrorLoadingMap(new Map(newRunDebugMessageLoadingMap))
        })
    }
  }, [job])

  let topLevelError = ""
  let topLevelErrorTitle = ""
  if (jobError != "") {
    topLevelError = jobError
    topLevelErrorTitle = "Job Error"
  } else {
    for (const run of job.runs) {
      const runErr = runErrorMap.get(run.runId) ?? ""
      if (runErr != "") {
        topLevelError = runErr
        topLevelErrorTitle = "Last Job Run Error"
      }
    }
  }

  useEffect(() => {
    mounted.current = true
    fetchRunErrors()
    fetchJobError()
    fetchRunDebugMessages()
    return () => {
      mounted.current = false
    }
  }, [job])

  const handleClickOpen = () => {
    setOpen(true)
  }

  const handleClose = () => {
    setOpen(false)
  }

  const getAccessToken = useGetAccessToken()

  const cordon = async (cluster: string, node: string) => {
    try {
      const accessToken = await getAccessToken()
      await cordonService.cordonNode(cluster, node, accessToken)
      openSnackbar("Successfully cordoned node " + node, "success")
    } catch (e) {
      const errMsg = await getErrorMessage(e)
      console.error(errMsg)
      if (!mounted.current) {
        return
      }
      openSnackbar("Failed to cordon node " + node + ": " + errMsg, "error")
    }
  }
  return (
    <>
      {topLevelError !== "" ? (
        <>
          <SidebarTabHeading>{topLevelErrorTitle}</SidebarTabHeading>
          <CodeBlock
            code={topLevelError}
            language="text"
            downloadable={false}
            showLineNumbers={false}
            loading={false}
          />
        </>
      ) : null}
      {runsNewestFirst.length === 0 ? (
        <NoRunsAlert jobState={job.state} />
      ) : (
        <>
          <SidebarTabHeading>Runs</SidebarTabHeading>
          <div>
            {runsNewestFirst.map((run, i) => {
              const node = run.node || undefined // set to undefined if it is an empty string
              const runErrorLoading = runErrorLoadingMap.get(run.runId) === "Loading"
              const runError = runErrorMap.get(run.runId) || undefined // set to undefined if it is an empty string
              const runDebugMessageLoading = runDebugMessageLoadingMap.get(run.runId) === "Loading"
              const runDebugMessage = runDebugMessageMap.get(run.runId) || undefined // set to undefined if it is an empty string

              const runIndicativeTimestamp = run.started || run.pending || run.leased || ""
              return (
                <Accordion key={run.runId} defaultExpanded={i === 0}>
                  <AccordionSummary>
                    <SidebarTabSubheading>
                      Run {i + 1}
                      {runIndicativeTimestamp && <>: {formatIsoTimestamp(runIndicativeTimestamp, "full")}</>} (
                      {formatJobRunState(run.jobRunState)})
                    </SidebarTabSubheading>
                  </AccordionSummary>
                  <AccordionDetails>
                    <KeyValuePairTable
                      data={[
                        { key: "Run ID", value: run.runId, allowCopy: true },
                        { key: "State", value: formatJobRunState(run.jobRunState) },
                        {
                          key: "Leased",
                          value: formatIsoTimestamp(run.leased, "full"),
                        },
                        {
                          key: "Pending",
                          value: formatIsoTimestamp(run.pending, "full"),
                        },
                        {
                          key: "Started",
                          value: formatIsoTimestamp(run.started, "full"),
                        },
                        {
                          key: "Finished",
                          value: formatIsoTimestamp(run.finished, "full"),
                        },
                        {
                          key: "Runtime",
                          value:
                            run.started && run.finished
                              ? formatDuration(
                                  (new Date(run.finished).getTime() - new Date(run.started).getTime()) / 1000,
                                )
                              : "",
                        },
                        { key: "Cluster", value: run.cluster, allowCopy: true },
                        { key: "Node", value: run.node ?? "", allowCopy: true },
                        { key: "Exit code", value: run.exitCode?.toString() ?? "" },
                      ].filter((pair) => pair.value !== "")}
                    />
                    {node && (
                      <>
                        <MarkNodeUnschedulableButtonContainer>
                          <Tooltip title={`Take node ${node} out of the farm`} placement="bottom">
                            <Button color="secondary" onClick={handleClickOpen} variant="outlined">
                              Mark node as unschedulable
                            </Button>
                          </Tooltip>
                        </MarkNodeUnschedulableButtonContainer>
                        <Dialog open={open} onClose={handleClose} fullWidth maxWidth="md">
                          <ErrorBoundary FallbackComponent={AlertErrorFallback}>
                            <DialogTitle>Mark node as unschedulable</DialogTitle>
                            <DialogContent>
                              <Typography>
                                Are you sure you want to take node <span>{node}</span> out of the farm?
                              </Typography>
                            </DialogContent>
                            <DialogActions>
                              <Button color="error" onClick={handleClose}>
                                Cancel
                              </Button>
                              <Button
                                onClick={async () => {
                                  await cordon(run.cluster, node)
                                  handleClose()
                                }}
                                autoFocus
                              >
                                Confirm
                              </Button>
                            </DialogActions>
                          </ErrorBoundary>
                        </Dialog>
                      </>
                    )}
                  </AccordionDetails>
                  {runErrorLoading && <div>{loadingAccordion}</div>}
                  {runError && (
                    <Accordion key={run.runId + "error"}>
                      <AccordionSummary>Error</AccordionSummary>
                      <AccordionDetails>
                        <CodeBlock
                          code={runError}
                          language="text"
                          downloadable={false}
                          showLineNumbers={false}
                          loading={false}
                        />
                      </AccordionDetails>
                    </Accordion>
                  )}
                  {runDebugMessageLoading && <div>{loadingAccordion}</div>}
                  {runDebugMessage && (
                    <Accordion key={run.runId + "debug"}>
                      <AccordionSummary expandIcon={<ExpandMore />} aria-controls="panel1d-content" id="panel1d-header">
                        Debug
                      </AccordionSummary>
                      <AccordionDetails>
                        <CodeBlock
                          code={runDebugMessage}
                          language="text"
                          downloadable={false}
                          showLineNumbers={false}
                          loading={false}
                        />
                      </AccordionDetails>
                    </Accordion>
                  )}
                </Accordion>
              )
            })}
          </div>
        </>
      )}
    </>
  )
}
