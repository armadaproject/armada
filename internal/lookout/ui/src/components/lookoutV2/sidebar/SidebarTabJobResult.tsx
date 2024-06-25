import React, { useCallback, useEffect, useMemo, useRef, useState } from "react"

import { ExpandMore } from "@mui/icons-material"
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Button,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Tooltip,
  Typography,
} from "@mui/material"
import { Job, JobRun, JobState } from "models/lookoutV2Models"
import { formatJobRunState, formatTimeSince, formatUtcDate } from "utils/jobsTableFormatters"

import { CodeBlock } from "./CodeBlock"
import { KeyValuePairTable } from "./KeyValuePairTable"
import styles from "./SidebarTabJobResult.module.css"
import { useCustomSnackbar } from "../../../hooks/useCustomSnackbar"
import { getAccessToken, useUserManager } from "../../../oidc"
import { ICordonService } from "../../../services/lookoutV2/CordonService"
import { IGetJobInfoService } from "../../../services/lookoutV2/GetJobInfoService"
import { IGetRunInfoService } from "../../../services/lookoutV2/GetRunInfoService"
import { getErrorMessage } from "../../../utils"

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

  const userManager = useUserManager()

  const cordon = async (cluster: string, node: string) => {
    try {
      const accessToken = userManager && (await getAccessToken(userManager))
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
    <div style={{ width: "100%", height: "100%" }}>
      {topLevelError !== "" ? (
        <>
          <Typography variant="subtitle2">{topLevelErrorTitle}:</Typography>
          <CodeBlock text={topLevelError} />
        </>
      ) : null}
      <Typography variant="subtitle2">Runs:</Typography>
      {runsNewestFirst.map((run, i) => {
        return (
          <Accordion key={run.runId} defaultExpanded={i === 0}>
            <AccordionSummary expandIcon={<ExpandMore />} aria-controls="panel1a-content">
              <Typography>
                {formatUtcDate(getRunScheduledTime(run))} UTC ({formatJobRunState(run.jobRunState)})
              </Typography>
            </AccordionSummary>
            <AccordionDetails sx={{ padding: 0 }}>
              <KeyValuePairTable
                data={[
                  { key: "Run ID", value: run.runId },
                  { key: "State", value: formatJobRunState(run.jobRunState) },
                  { key: "Leased (UTC)", value: formatUtcDate(run.leased) },
                  { key: "Pending (UTC)", value: formatUtcDate(run.pending) },
                  { key: "Started (UTC)", value: formatUtcDate(run.started) },
                  { key: "Finished (UTC)", value: formatUtcDate(run.finished) },
                  {
                    key: "Runtime",
                    value:
                      run.started && run.finished ? formatTimeSince(run.started, new Date(run.finished).getTime()) : "",
                  },
                  { key: "Cluster", value: run.cluster },
                  { key: "Node", value: run.node ?? "" },
                  { key: "Exit code", value: run.exitCode?.toString() ?? "" },
                ].filter((pair) => pair.value !== "")}
              />
            </AccordionDetails>
            {run.node !== undefined && run.node !== "" && (
              <>
                <div className={styles.markUnschedulableButton}>
                  <Tooltip title={`Take node ${run.node} out of the farm`} placement="bottom">
                    <Button color="secondary" onClick={handleClickOpen}>
                      Mark node as unschedulable
                    </Button>
                  </Tooltip>
                  <Dialog
                    open={open}
                    onClose={handleClose}
                    fullWidth
                    maxWidth="md"
                    className={styles.markUnschedulableDialog}
                  >
                    <DialogTitle>Mark node as unschedulable</DialogTitle>
                    <DialogContent>
                      <Typography>
                        Are you sure you want to take node <span>{run.node}</span> out of the farm?
                      </Typography>
                    </DialogContent>
                    <DialogActions>
                      <Button color="error" onClick={handleClose}>
                        Cancel
                      </Button>
                      <Button
                        onClick={async () => {
                          await cordon(run.cluster, run.node as string)
                          handleClose()
                        }}
                        autoFocus
                      >
                        Confirm
                      </Button>
                    </DialogActions>
                  </Dialog>
                </div>
              </>
            )}
            {runErrorLoadingMap.has(run.runId) && runErrorLoadingMap.get(run.runId) === "Loading" && (
              <div className={styles.loading}>
                <CircularProgress size={24} />
              </div>
            )}
            {runErrorMap.has(run.runId) && runErrorMap.get(run.runId) !== "" && (
              <Accordion key={run.runId + "error"}>
                <AccordionSummary expandIcon={<ExpandMore />} aria-controls="panel1d-content" id="panel1d-header">
                  Error
                </AccordionSummary>
                <AccordionDetails>{<CodeBlock text={runErrorMap.get(run.runId) ?? ""} />}</AccordionDetails>
              </Accordion>
            )}
            {runDebugMessageLoadingMap.has(run.runId) && runDebugMessageLoadingMap.get(run.runId) === "Loading" && (
              <div className={styles.loading}>
                <CircularProgress size={24} />
              </div>
            )}
            {runDebugMessageMap.has(run.runId) && runDebugMessageMap.get(run.runId) !== "" && (
              <Accordion key={run.runId + "debug"}>
                <AccordionSummary expandIcon={<ExpandMore />} aria-controls="panel1d-content" id="panel1d-header">
                  Debug
                </AccordionSummary>
                <AccordionDetails>{<CodeBlock text={runDebugMessageMap.get(run.runId) ?? ""} />}</AccordionDetails>
              </Accordion>
            )}
          </Accordion>
        )
      })}
      {runsNewestFirst.length === 0 && <>This job has not run.</>}
    </div>
  )
}

function getRunScheduledTime(run: JobRun): string {
  if (run.leased !== undefined && run.leased !== "") {
    return run.leased
  }
  if (run.pending !== undefined && run.pending !== "") {
    return run.pending
  }
  return ""
}
