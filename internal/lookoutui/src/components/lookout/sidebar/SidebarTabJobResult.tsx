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
import { ErrorBoundary } from "react-error-boundary"

import { KeyValuePairTable } from "./KeyValuePairTable"
import { NoRunsAlert } from "./NoRunsAlert"
import { SidebarTabHeading, SidebarTabSubheading } from "./sidebarTabContentComponents"
import { formatDuration } from "../../../common/formatTime"
import { useFormatIsoTimestampWithUserSettings } from "../../../hooks/formatTimeWithUserSettings"
import { useCustomSnackbar } from "../../../hooks/useCustomSnackbar"
import { Job, JobState } from "../../../models/lookoutModels"
import { useAuthenticatedFetch } from "../../../oidcAuth"
import { IGetJobInfoService } from "../../../services/lookout/GetJobInfoService"
import { useCordonNode } from "../../../services/lookout/useCordonNode"
import { useBatchGetJobRunDebugMessages } from "../../../services/lookout/useGetJobRunDebugMessage"
import { useBatchGetJobRunErrors } from "../../../services/lookout/useGetJobRunError"
import { SPACING } from "../../../styling/spacing"
import { getErrorMessage } from "../../../utils"
import { formatJobRunState } from "../../../utils/jobsTableFormatters"
import { AlertErrorFallback } from "../../AlertErrorFallback"
import { CodeBlock } from "../../CodeBlock"

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
  jobInfoService: IGetJobInfoService
}

export const SidebarTabJobResult = ({ job, jobInfoService }: SidebarTabJobResultProps) => {
  const mounted = useRef(false)
  const openSnackbar = useCustomSnackbar()

  const formatIsoTimestamp = useFormatIsoTimestampWithUserSettings()

  const runsNewestFirst = useMemo(() => [...job.runs].reverse(), [job])
  const [jobError, setJobError] = useState<string>("")
  const [open, setOpen] = useState(false)
  const authenticatedFetch = useAuthenticatedFetch()

  const fetchJobError = useCallback(async () => {
    if (job.state != JobState.Failed && job.state != JobState.Rejected) {
      setJobError("")
      return
    }
    const getJobErrorResultPromise = jobInfoService.getJobError(authenticatedFetch, job.jobId)
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

  const batchGetJobRunErrorResults = useBatchGetJobRunErrors((job.runs ?? []).map(({ runId }) => runId))
  useEffect(() => {
    batchGetJobRunErrorResults.forEach(({ status, error }, i) => {
      if (status === "error") {
        openSnackbar(
          `Failed to retrieve Job Run error for Run with ID: ${(job.runs ?? [])[i].runId}: ${error.message}`,
          "error",
        )
      }
    })
  }, [batchGetJobRunErrorResults, openSnackbar])

  const batchGetJobRunDebugMessagesResults = useBatchGetJobRunDebugMessages((job.runs ?? []).map(({ runId }) => runId))
  useEffect(() => {
    batchGetJobRunDebugMessagesResults.forEach(({ status, error }, i) => {
      if (status === "error") {
        openSnackbar(
          `Failed to retrieve Job Run debug message for Run with ID: ${(job.runs ?? [])[i].runId}: ${error.message}`,
          "error",
        )
      }
    })
  }, [batchGetJobRunDebugMessagesResults, openSnackbar])

  let topLevelError = ""
  let topLevelErrorTitle = ""
  if (jobError != "") {
    topLevelError = jobError
    topLevelErrorTitle = "Job Error"
  } else {
    job.runs.forEach((_, i) => {
      if (batchGetJobRunErrorResults[i]?.data) {
        topLevelError = batchGetJobRunErrorResults[i].data
        topLevelErrorTitle = "Last Job Run Error"
      }
    })
  }

  useEffect(() => {
    mounted.current = true
    fetchJobError()
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

  const cordonNode = useCordonNode()

  const cordon = async (cluster: string, node: string) => {
    try {
      await cordonNode.mutateAsync({ cluster, node })
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
              const runErrorLoading = batchGetJobRunErrorResults[i]?.status === "pending"
              const runError = batchGetJobRunErrorResults[i]?.data
              const runDebugMessageLoading = batchGetJobRunDebugMessagesResults[i]?.status === "pending"
              const runDebugMessage = batchGetJobRunDebugMessagesResults[i]?.data

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
