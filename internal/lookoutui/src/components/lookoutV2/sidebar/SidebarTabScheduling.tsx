import { useState } from "react"

import { Refresh } from "@mui/icons-material"
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Alert,
  AlertTitle,
  Button,
  Stack,
  Typography,
} from "@mui/material"

import { KeyValuePairTable } from "./KeyValuePairTable"
import { SidebarTabHeading, SidebarTabProminentValueCard, SidebarTabSubheading } from "./sidebarTabContentComponents"
import { Job, JobState } from "../../../models/lookoutV2Models"
import { useGetJobSchedulingReport } from "../../../services/lookoutV2/useGetJobSchedulingReport"
import { useGetQueueSchedulingReport } from "../../../services/lookoutV2/useGetQueueSchedulingReport"
import { useGetSchedulingReport } from "../../../services/lookoutV2/useGetSchedulingReport"
import { SPACING } from "../../../styling/spacing"
import { formatTimeSince } from "../../../utils/jobsTableFormatters"
import { formatCpu, formatBytes } from "../../../utils/resourceUtils"
import { CodeBlock } from "../../CodeBlock"
import { VerbositySelector } from "../../VerbositySelector"

export interface SidebarTabSchedulingProps {
  job: Job
}

export const SidebarTabScheduling = ({ job }: SidebarTabSchedulingProps) => {
  const [showAdvancedReports, setShowAdvancedReports] = useState(false)
  const [queueReportVerbosity, setQueueReportVerbosity] = useState(0)
  const [schedulingReportVerbosity, setSchedulingReportVerbosity] = useState(0)

  const getJobSchedulingReportResult = useGetJobSchedulingReport(job.jobId, Boolean(job.jobId))
  const getQueueSchedulingReportResult = useGetQueueSchedulingReport(
    job.queue,
    queueReportVerbosity,
    showAdvancedReports && Boolean(job.queue),
  )
  const getSchedulingReportResult = useGetSchedulingReport(schedulingReportVerbosity, showAdvancedReports)

  return (
    <>
      <Stack spacing={SPACING.md} direction="row">
        {job.state === JobState.Queued && (
          <SidebarTabProminentValueCard label="Time queued" value={formatTimeSince(job.lastTransitionTime)} />
        )}
      </Stack>
      <SidebarTabHeading>Scheduling report</SidebarTabHeading>
      <Typography>This is the scheduling report for this job from the latest scheduling round.</Typography>
      {getJobSchedulingReportResult.status === "error" && (
        <Alert
          severity="error"
          action={
            <Button color="inherit" size="small" onClick={() => getJobSchedulingReportResult.refetch()}>
              Retry
            </Button>
          }
        >
          <AlertTitle>Failed to get the scheduling report for this job.</AlertTitle>
          {getJobSchedulingReportResult.error}
        </Alert>
      )}
      {(getJobSchedulingReportResult.status === "pending" || getJobSchedulingReportResult.isFetching) && (
        <CodeBlock
          language="text"
          loading
          downloadable={false}
          showLineNumbers={false}
          loadingLines={5}
          loadingLineLength={60}
        />
      )}
      {getJobSchedulingReportResult.status === "success" &&
        !getJobSchedulingReportResult.isFetching &&
        (getJobSchedulingReportResult.data.report ? (
          <CodeBlock
            language="text"
            code={getJobSchedulingReportResult.data.report ?? ""}
            loading={false}
            downloadable={false}
            showLineNumbers={false}
            additionalActions={[
              { title: "Refetch", onClick: () => getJobSchedulingReportResult.refetch(), icon: <Refresh /> },
            ]}
          />
        ) : (
          <Alert
            severity="info"
            variant="outlined"
            action={
              <Button color="inherit" size="small" onClick={() => getJobSchedulingReportResult.refetch()}>
                Retry
              </Button>
            }
          >
            No scheduling report is available for this job.
          </Alert>
        ))}
      <div>
        <Accordion expanded={showAdvancedReports} onChange={(_, expanded) => setShowAdvancedReports(expanded)}>
          <AccordionSummary>
            <Typography>Advanced reports</Typography>
          </AccordionSummary>
          <AccordionDetails>
            <Stack spacing={SPACING.md}>
              <SidebarTabSubheading>Report for this job's queue ({job.queue})</SidebarTabSubheading>
              <Stack spacing={SPACING.sm}>
                <VerbositySelector
                  name="queue-scheduling-report-verbosity"
                  legendLabel="Report verbosity"
                  max={3}
                  verbosity={queueReportVerbosity}
                  onChange={(verbosity) => setQueueReportVerbosity(verbosity)}
                  disabled={
                    getQueueSchedulingReportResult.status === "pending" || getQueueSchedulingReportResult.isFetching
                  }
                />
                {getQueueSchedulingReportResult.status === "error" && (
                  <Alert
                    severity="error"
                    action={
                      <Button color="inherit" size="small" onClick={() => getQueueSchedulingReportResult.refetch()}>
                        Retry
                      </Button>
                    }
                  >
                    <AlertTitle>Failed to get the scheduling report for this queue.</AlertTitle>
                    {getQueueSchedulingReportResult.error}
                  </Alert>
                )}
                {(getQueueSchedulingReportResult.status === "pending" || getQueueSchedulingReportResult.isFetching) && (
                  <CodeBlock
                    language="text"
                    loading
                    downloadable={false}
                    showLineNumbers={false}
                    loadingLines={5}
                    loadingLineLength={60}
                  />
                )}
                {getQueueSchedulingReportResult.status === "success" &&
                  !getQueueSchedulingReportResult.isFetching &&
                  (getQueueSchedulingReportResult.data.report ? (
                    <CodeBlock
                      language="text"
                      code={getQueueSchedulingReportResult.data.report ?? ""}
                      loading={false}
                      downloadable={false}
                      showLineNumbers={false}
                      additionalActions={[
                        {
                          title: "Refetch",
                          onClick: () => getQueueSchedulingReportResult.refetch(),
                          icon: <Refresh />,
                        },
                      ]}
                    />
                  ) : (
                    <Alert
                      severity="info"
                      variant="outlined"
                      action={
                        <Button color="inherit" size="small" onClick={() => getQueueSchedulingReportResult.refetch()}>
                          Retry
                        </Button>
                      }
                    >
                      No scheduling report is available for this queue.
                    </Alert>
                  ))}
              </Stack>
              <SidebarTabSubheading>Overall scheduling report</SidebarTabSubheading>
              <Stack spacing={SPACING.sm}>
                <VerbositySelector
                  name="scheduling-report-verbosity"
                  legendLabel="Report verbosity"
                  max={3}
                  verbosity={schedulingReportVerbosity}
                  onChange={(verbosity) => setSchedulingReportVerbosity(verbosity)}
                  disabled={getSchedulingReportResult.status === "pending" || getSchedulingReportResult.isFetching}
                />
                {getSchedulingReportResult.status === "error" && (
                  <Alert
                    severity="error"
                    action={
                      <Button color="inherit" size="small" onClick={() => getSchedulingReportResult.refetch()}>
                        Retry
                      </Button>
                    }
                  >
                    <AlertTitle>Failed to get the overall scheduling report.</AlertTitle>
                    {getSchedulingReportResult.error}
                  </Alert>
                )}
                {(getSchedulingReportResult.status === "pending" || getSchedulingReportResult.isFetching) && (
                  <CodeBlock
                    language="text"
                    loading
                    downloadable={false}
                    showLineNumbers={false}
                    loadingLines={5}
                    loadingLineLength={60}
                  />
                )}
                {getSchedulingReportResult.status === "success" &&
                  !getSchedulingReportResult.isFetching &&
                  (getSchedulingReportResult.data.report ? (
                    <CodeBlock
                      language="text"
                      code={getSchedulingReportResult.data.report ?? ""}
                      loading={false}
                      downloadable={false}
                      showLineNumbers={false}
                      additionalActions={[
                        { title: "Refetch", onClick: () => getSchedulingReportResult.refetch(), icon: <Refresh /> },
                      ]}
                    />
                  ) : (
                    <Alert
                      severity="info"
                      variant="outlined"
                      action={
                        <Button color="inherit" size="small" onClick={() => getSchedulingReportResult.refetch()}>
                          Retry
                        </Button>
                      }
                    >
                      No scheduling report is available.
                    </Alert>
                  ))}
              </Stack>
            </Stack>
          </AccordionDetails>
        </Accordion>
      </div>
      <SidebarTabHeading>Scheduling parameters</SidebarTabHeading>
      <Alert severity="info" variant="outlined">
        These job details are used by Armada's scheduler when determining which jobs to execute on the cluster.
      </Alert>
      <Stack spacing={SPACING.md} direction="row">
        <SidebarTabProminentValueCard label="Job priority" value={job.priority.toString()} />
      </Stack>
      <SidebarTabSubheading>Resource requests</SidebarTabSubheading>
      <KeyValuePairTable
        data={[
          { key: "CPUs", value: formatCpu(job.cpu) },
          { key: "Memory", value: formatBytes(job.memory) },
          { key: "GPUs", value: job.gpu.toString() },
          { key: "Ephemeral storage", value: formatBytes(job.ephemeralStorage) },
        ]}
      />
    </>
  )
}
