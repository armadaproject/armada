import React, { useCallback, useEffect, useMemo, useRef, useState } from "react"

import { ExpandMore } from "@mui/icons-material"
import {
  Accordion,
  AccordionSummary,
  Typography,
  AccordionDetails,
  CircularProgress,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
} from "@mui/material"
import { Button, Tooltip } from "@mui/material"
import { Job, JobRun } from "models/lookoutV2Models"
import { formatJobRunState, formatUtcDate } from "utils/jobsTableFormatters"

import { useCustomSnackbar } from "../../../hooks/useCustomSnackbar"
import { ICordonService } from "../../../services/lookoutV2/CordonService"
import { IGetRunErrorService } from "../../../services/lookoutV2/GetRunErrorService"
import { getErrorMessage } from "../../../utils"
import { CodeBlock } from "./CodeBlock"
import { KeyValuePairTable } from "./KeyValuePairTable"
import styles from "./SidebarTabJobRuns.module.css"

export interface SidebarTabJobRunsProps {
  job: Job
  runErrorService: IGetRunErrorService
  cordonService: ICordonService
}

type LoadState = "Idle" | "Loading"

export const SidebarTabJobRuns = ({ job, runErrorService, cordonService }: SidebarTabJobRunsProps) => {
  const mounted = useRef(false)
  const openSnackbar = useCustomSnackbar()
  const runsNewestFirst = useMemo(() => [...job.runs].reverse(), [job])
  const [runErrorMap, setRunErrorMap] = useState<Map<string, string>>(new Map<string, string>())
  const [runErrorLoadingMap, setRunErrorLoadingMap] = useState<Map<string, LoadState>>(new Map<string, LoadState>())
  const [open, setOpen] = useState(false)

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
        promise: runErrorService.getRunError(run.runId, undefined),
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

  useEffect(() => {
    mounted.current = true
    fetchRunErrors()
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

  const cordon = async (cluster: string, node: string) => {
    try {
      await cordonService.cordonNode(cluster, node, undefined)
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
          </Accordion>
        )
      })}
      {runsNewestFirst.length === 0 && <>This job has not run.</>}
    </div>
  )
}

function getRunScheduledTime(run: JobRun): string {
  if (run.pending !== undefined && run.pending !== "") {
    return run.pending
  }
  if (run.leased !== undefined && run.leased !== "") {
    return run.leased
  }
  return ""
}
