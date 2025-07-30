import { useMemo, useState } from "react"

import { Alert, AlertTitle, Button } from "@mui/material"

import { JobRunDetails } from "./JobRunDetails"
import { NoRunsAlert } from "./NoRunsAlert"
import { SidebarTabHeading } from "./sidebarTabContentComponents"
import { Job, JobState } from "../../../models/lookoutModels"
import { useGetJobError } from "../../../services/lookout/useGetJobError"
import { CodeBlock } from "../../CodeBlock"

export interface SidebarTabJobResultProps {
  job: Job
}

export const SidebarTabJobResult = ({ job }: SidebarTabJobResultProps) => {
  const [lastJobRunError, setLastJobRunError] = useState<string | undefined>(undefined)

  const runsNewestFirst = useMemo(() => [...job.runs].reverse(), [job])

  const {
    data: jobError,
    status: jobErrorStatus,
    error: jobErrorError,
    refetch: refetchJobError,
  } = useGetJobError(job.jobId, job.state == JobState.Failed || job.state == JobState.Rejected)

  const topLevelError = useMemo<{ title: string; content: string } | undefined>(() => {
    if (jobErrorStatus === "success" && jobError) {
      return { title: "Job Error", content: jobError }
    }
    if (lastJobRunError) {
      return { title: "Last Job Run Error", content: lastJobRunError }
    }
    return undefined
  }, [jobErrorStatus, jobError, lastJobRunError])

  return (
    <>
      {jobErrorStatus === "error" && (
        <Alert
          severity="error"
          action={
            <Button color="inherit" size="small" onClick={() => refetchJobError()}>
              Retry
            </Button>
          }
        >
          <AlertTitle>Failed to list available queues.</AlertTitle>
          {jobErrorError}
        </Alert>
      )}
      {topLevelError && (
        <>
          <SidebarTabHeading>{topLevelError.title}</SidebarTabHeading>
          <CodeBlock
            code={topLevelError.content}
            language="text"
            downloadable={false}
            showLineNumbers={false}
            loading={false}
          />
        </>
      )}
      {runsNewestFirst.length === 0 ? (
        <NoRunsAlert jobState={job.state} />
      ) : (
        <>
          <SidebarTabHeading>Runs</SidebarTabHeading>
          <div>
            {runsNewestFirst.map((run, i) => (
              <JobRunDetails
                key={run.runId}
                run={run}
                runIndex={runsNewestFirst.length - 1 - i}
                defaultExpanded={i === 0}
                setRunError={i === 0 ? setLastJobRunError : undefined}
              />
            ))}
          </div>
        </>
      )}
    </>
  )
}
