import React, { useCallback, useEffect, useRef, useState } from "react"

import { Accordion, AccordionSummary, Typography, AccordionDetails, CircularProgress } from "@material-ui/core"
import { ExpandMore } from "@mui/icons-material"
import { Job } from "models/lookoutV2Models"
import { formatJobRunState, formatUtcDate } from "utils/jobsTableFormatters"

import { useCustomSnackbar } from "../../../hooks/useCustomSnackbar"
import { IGetRunErrorService } from "../../../services/lookoutV2/GetRunErrorService"
import { getErrorMessage } from "../../../utils"
import { CodeBlock } from "./CodeBlock"
import { KeyValuePairTable } from "./KeyValuePairTable"
import styles from "./SidebarTabJobRuns.module.css"

export interface SidebarTabJobRuns {
  job: Job
  runErrorService: IGetRunErrorService
}

type LoadState = "Idle" | "Loading"

export const SidebarTabJobRuns = ({ job, runErrorService }: SidebarTabJobRuns) => {
  const mounted = useRef(false)
  const openSnackbar = useCustomSnackbar()
  const runsNewestFirst = [...job.runs].reverse()
  const [runErrorMap, setRunErrorMap] = useState<Map<string, string>>(new Map<string, string>())
  const [runErrorLoadingMap, setRunErrorLoadingMap] = useState<Map<string, LoadState>>(new Map<string, LoadState>())

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

  return (
    <>
      {runsNewestFirst.map((run) => {
        return (
          <Accordion key={run.runId}>
            <AccordionSummary expandIcon={<ExpandMore />} aria-controls="panel1a-content">
              <Typography>
                {formatUtcDate(run.pending)} UTC ({formatJobRunState(run.jobRunState)})
              </Typography>
            </AccordionSummary>
            <AccordionDetails>
              <KeyValuePairTable
                data={[
                  { key: "Run ID", value: run.runId },
                  { key: "State", value: formatJobRunState(run.jobRunState) },
                  { key: "Pending (UTC)", value: formatUtcDate(run.pending) },
                  { key: "Started (UTC)", value: formatUtcDate(run.started) },
                  { key: "Finished (UTC)", value: formatUtcDate(run.finished) },
                  { key: "Cluster", value: run.cluster },
                  { key: "Node", value: run.node ?? "" },
                  { key: "Exit code", value: run.exitCode?.toString() ?? "" },
                ].filter((pair) => pair.value !== "")}
              />
            </AccordionDetails>
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
    </>
  )
}
