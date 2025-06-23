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
import { useEffect, useMemo, useState } from "react"
import { ErrorBoundary } from "react-error-boundary"

import { formatDuration, TimestampFormat } from "../../../common/formatTime"
import { useFormatIsoTimestampWithUserSettings } from "../../../hooks/formatTimeWithUserSettings"
import { useCustomSnackbar } from "../../../hooks/useCustomSnackbar"
import { JobRun } from "../../../models/lookoutModels"
import { useCordonNode } from "../../../services/lookout/useCordonNode"
import { useGetJobRunDebugMessage } from "../../../services/lookout/useGetJobRunDebugMessage"
import { useGetJobRunError } from "../../../services/lookout/useGetJobRunError"
import { SPACING } from "../../../styling/spacing"
import { formatJobRunState } from "../../../utils/jobsTableFormatters"
import { AlertErrorFallback } from "../../AlertErrorFallback"
import { CodeBlock } from "../../CodeBlock"
import { KeyValuePairTable } from "./KeyValuePairTable"
import { SidebarTabSubheading } from "./sidebarTabContentComponents"

const MarkNodeUnschedulableButtonContainer = styled("div")(({ theme }) => ({
  display: "flex",
  justifyContent: "center",
  padding: theme.spacing(SPACING.sm),
}))

const makeKeyValuePairsData = (
  formatIsoTimestamp: (isoTimestampString: string | undefined, format: TimestampFormat) => string,
  {
    runId,
    jobRunState,
    leased,
    pending,
    started,
    finished,
    exitCode,
    cluster,
    node,
  }: Pick<
    JobRun,
    "runId" | "jobRunState" | "leased" | "pending" | "started" | "finished" | "exitCode" | "cluster" | "node"
  >,
): KeyValuePairTable["data"] => {
  const d = [] as KeyValuePairTable["data"]

  if (runId) {
    d.push({ key: "Run ID", value: runId, allowCopy: true })
  }
  if (jobRunState) {
    d.push({ key: "State", value: formatJobRunState(jobRunState) })
  }
  if (leased) {
    d.push({ key: "Leased", value: formatIsoTimestamp(leased, "full") })
  }
  if (pending) {
    d.push({ key: "Pending", value: formatIsoTimestamp(pending, "full") })
  }
  if (started) {
    d.push({ key: "Started", value: formatIsoTimestamp(started, "full") })
  }
  if (finished) {
    d.push({ key: "Finished", value: formatIsoTimestamp(finished, "full") })
  }
  if (started && finished) {
    const runtimeMs = new Date(finished).getTime() - new Date(started).getTime()
    d.push({ key: "Runtime", value: formatDuration(runtimeMs / 1000) })
  }
  if (exitCode !== undefined) {
    d.push({ key: "Exit code", value: exitCode.toString() })
  }
  if (cluster) {
    d.push({ key: "Cluster", value: cluster, allowCopy: true })
  }
  if (node) {
    d.push({ key: "Node", value: node, allowCopy: true })
  }

  return d
}

export interface JobRunDetailsProps {
  run: JobRun
  runIndex: number
  defaultExpanded: boolean
  setRunError: undefined | ((runError: string) => void)
}

export const JobRunDetails = ({
  run: { node, cluster, started, pending, leased, finished, jobRunState, runId, exitCode },
  runIndex,
  defaultExpanded,
  setRunError,
}: JobRunDetailsProps) => {
  const formatIsoTimestamp = useFormatIsoTimestampWithUserSettings()
  const openSnackbar = useCustomSnackbar()

  const [markUnschedulableDialogOpen, setMarkUnschedulableDialogOpen] = useState(false)

  const { mutateAsync: cordonNode, status: cordonNodeStatus, error: cordonNodeError } = useCordonNode()

  useEffect(() => {
    if (cordonNodeStatus === "success") {
      openSnackbar(`Successfully cordoned node ${node}`, "success")
    }
    if (cordonNodeStatus === "error") {
      openSnackbar(`Failed to cordon node ${node}: ${cordonNodeError}`, "error")
    }
  }, [cordonNodeStatus, cordonNodeError, node])

  // Ignore any errors from this call - the API returns an error if there is no error message, which is happy-path
  const { data: runError, status: runErrorStatus } = useGetJobRunError(runId)
  useEffect(() => {
    if (setRunError && runErrorStatus === "success") {
      setRunError(runError)
    }
  }, [setRunError, runError, runErrorStatus])

  // Ignore any errors from this call - the API returns an error if there is no debug message, which is happy-path
  const { data: debugMessage, status: debugMessageStatus } = useGetJobRunDebugMessage(runId)

  const headingTextParts = ["Run", (runIndex + 1).toString()]
  const runIndicativeTimestamp = started || pending || leased || ""
  if (runIndicativeTimestamp) {
    headingTextParts.push(formatIsoTimestamp(runIndicativeTimestamp, "full"))
  }
  headingTextParts.push(`(${formatJobRunState(jobRunState)})`)
  const headingText = headingTextParts.join(" ")

  const tableData = useMemo(
    () =>
      makeKeyValuePairsData(formatIsoTimestamp, {
        runId,
        jobRunState,
        leased,
        pending,
        started,
        finished,
        exitCode,
        cluster,
        node,
      }),
    [formatIsoTimestamp, runId, jobRunState, leased, pending, started, finished, exitCode, cluster, node],
  )

  return (
    <Accordion defaultExpanded={defaultExpanded}>
      <AccordionSummary>
        <SidebarTabSubheading>{headingText}</SidebarTabSubheading>
      </AccordionSummary>
      <AccordionDetails>
        <KeyValuePairTable data={tableData} />
        {node && (
          <>
            <MarkNodeUnschedulableButtonContainer>
              <Tooltip title={`Take node ${node} out of the farm`} placement="bottom">
                <Button color="secondary" onClick={() => setMarkUnschedulableDialogOpen(true)} variant="outlined">
                  Mark node as unschedulable
                </Button>
              </Tooltip>
            </MarkNodeUnschedulableButtonContainer>
            <Dialog
              open={markUnschedulableDialogOpen}
              onClose={() => setMarkUnschedulableDialogOpen(false)}
              fullWidth
              maxWidth="md"
            >
              <ErrorBoundary FallbackComponent={AlertErrorFallback}>
                <DialogTitle>Mark node as unschedulable</DialogTitle>
                <DialogContent>
                  <Typography>
                    Are you sure you want to take node <strong>{node}</strong> out of the farm?
                  </Typography>
                </DialogContent>
                <DialogActions>
                  <Button color="error" onClick={() => setMarkUnschedulableDialogOpen(false)}>
                    Cancel
                  </Button>
                  <Button
                    onClick={async () => {
                      await cordonNode({ cluster: cluster, node: node })
                      setMarkUnschedulableDialogOpen(false)
                    }}
                    autoFocus
                    loading={cordonNodeStatus === "pending"}
                  >
                    Confirm
                  </Button>
                </DialogActions>
              </ErrorBoundary>
            </Dialog>
          </>
        )}
        <div>
          {runErrorStatus === "pending" && (
            <Accordion variant="elevation" square>
              <AccordionSummary disabled>
                <Skeleton width="5ch" />
              </AccordionSummary>
            </Accordion>
          )}
          {runErrorStatus === "success" && runError && (
            <Accordion variant="elevation" square>
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
          {debugMessageStatus === "pending" && (
            <Accordion variant="elevation" square>
              <AccordionSummary disabled>
                <Skeleton width="5ch" />
              </AccordionSummary>
            </Accordion>
          )}
          {debugMessageStatus === "success" && debugMessage && (
            <Accordion variant="elevation" square>
              <AccordionSummary>Debug</AccordionSummary>
              <AccordionDetails>
                <CodeBlock
                  code={debugMessage}
                  language="text"
                  downloadable={false}
                  showLineNumbers={false}
                  loading={false}
                />
              </AccordionDetails>
            </Accordion>
          )}
        </div>
      </AccordionDetails>
    </Accordion>
  )
}
