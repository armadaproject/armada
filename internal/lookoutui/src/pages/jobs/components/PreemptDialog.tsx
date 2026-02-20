import { ChangeEvent, useCallback, useEffect, useMemo, useState } from "react"

import { Refresh, Dangerous } from "@mui/icons-material"
import {
  Button,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Alert,
  TextField,
} from "@mui/material"
import { ErrorBoundary } from "react-error-boundary"

import { formatJobState } from "../../../common/jobsTableFormatters"
import { waitMs } from "../../../common/utils"
import { AlertErrorFallback } from "../../../components/AlertErrorFallback"
import { useFormatNumberWithUserSettings } from "../../../components/hooks/formatNumberWithUserSettings"
import { useFormatIsoTimestampWithUserSettings } from "../../../components/hooks/formatTimeWithUserSettings"
import { useCustomSnackbar } from "../../../components/hooks/useCustomSnackbar"
import { isTerminatedJobState, Job, JobFiltersWithExcludes, JobId } from "../../../models/lookoutModels"
import { useGetAllJobsMatchingFilters } from "../../../services/lookout/useGetAllJobsMatchingFilters"
import { usePreemptJobs } from "../../../services/lookout/usePreemptJobs"

import dialogStyles from "./DialogStyles.module.css"
import { JobStatusTable } from "./JobStatusTable"

const MAX_JOB_IDS_TO_DISPLAY = 4

interface PreemptDialogProps {
  onClose: () => void
  selectedItemFilters: JobFiltersWithExcludes[]
}

export const PreemptDialog = ({ onClose, selectedItemFilters }: PreemptDialogProps) => {
  // State
  const [jobIdsToPreemptResponses, setJobIdsToPreemptResponses] = useState<Record<JobId, string>>({})
  const [preemptReason, setPreemptReason] = useState<string | undefined>(undefined)
  const [isPreempting, setIsPreempting] = useState(false)
  const [hasAttemptedPreempt, setHasAttemptedPreempt] = useState(false)
  const [refetchAfterPreempt, setRefetchAfterPreempt] = useState(false)
  const openSnackbar = useCustomSnackbar()

  const formatIsoTimestamp = useFormatIsoTimestampWithUserSettings()
  const preemptJobsMutation = usePreemptJobs()

  // Fetch all jobs matching the filters using the new hook
  const {
    data: selectedJobs,
    isLoading: isLoadingJobs,
    error,
    refetch,
  } = useGetAllJobsMatchingFilters({
    filtersGroups: selectedItemFilters,
    activeJobSets: false,
    enabled: true,
  })

  const preemptibleJobs = useMemo(() => selectedJobs.filter((job) => !isTerminatedJobState(job.state)), [selectedJobs])

  // Actions
  const preemptJobs = useCallback(async () => {
    if (preemptReason === undefined) {
      return
    }

    setIsPreempting(true)

    try {
      const response = await preemptJobsMutation.mutateAsync({
        jobs: preemptibleJobs,
        preemptReason,
      })

      if (response.failedJobIds.length === 0) {
        const ids = response.successfulJobIds
        if (ids.length <= MAX_JOB_IDS_TO_DISPLAY) {
          openSnackbar(`Successfully requested preemption for: ${ids.join(", ")}`, "success")
        } else {
          const displayed = ids.slice(0, MAX_JOB_IDS_TO_DISPLAY).join(", ")
          openSnackbar(
            `Successfully requested preemption for ${ids.length} jobs: ${displayed}, and ${ids.length - MAX_JOB_IDS_TO_DISPLAY} more`,
            "success",
          )
        }
      } else {
        openSnackbar("Some preemption requests failed. See table for job statuses.", "warning")
      }

      const newResponseStatus = { ...jobIdsToPreemptResponses }

      response.successfulJobIds.forEach((jobId) => {
        newResponseStatus[jobId] = "Success"
      })
      response.failedJobIds.forEach(({ jobId, errorReason }) => {
        newResponseStatus[jobId] = errorReason
      })

      setJobIdsToPreemptResponses(newResponseStatus)
      setHasAttemptedPreempt(true)
    } finally {
      setIsPreempting(false)
    }
  }, [preemptReason, preemptibleJobs, jobIdsToPreemptResponses, preemptJobsMutation, openSnackbar])

  // Wait after preempt and refetch
  useEffect(() => {
    if (refetchAfterPreempt) {
      const doRefetch = async () => {
        await waitMs(500)
        refetch()
        setRefetchAfterPreempt(false)
      }
      doRefetch()
    }
  }, [refetchAfterPreempt, refetch])

  // Event handlers
  const handlePreemptReasonChange = useCallback((event: ChangeEvent<HTMLInputElement>) => {
    const reason = event.target.value
    if (reason.length > 0) {
      setPreemptReason(reason)
    } else {
      setPreemptReason(undefined)
    }
    setHasAttemptedPreempt(false)
  }, [])

  const handlePreemptJobs = useCallback(async () => {
    await preemptJobs()
    // Trigger a refetch after a small delay
    setRefetchAfterPreempt(true)
  }, [preemptJobs])

  const handleRefetch = useCallback(() => {
    setJobIdsToPreemptResponses({})
    setHasAttemptedPreempt(false)
    refetch()
  }, [refetch])

  const handleDialogKeyDown = useCallback(
    (event: React.KeyboardEvent) => {
      if (
        event.key === "Enter" &&
        !isLoadingJobs &&
        !hasAttemptedPreempt &&
        !isPreempting &&
        preemptibleJobs.length > 0
      ) {
        event.preventDefault()
        handlePreemptJobs()
      }
    },
    [isLoadingJobs, hasAttemptedPreempt, isPreempting, preemptibleJobs.length, handlePreemptJobs],
  )

  const jobsToRender = useMemo(() => preemptibleJobs.slice(0, 1000), [preemptibleJobs])
  const formatState = useCallback((job: Job) => formatJobState(job.state), [])
  const formatSubmittedTime = useCallback((job: Job) => formatIsoTimestamp(job.submitted, "full"), [formatIsoTimestamp])

  const formatNumber = useFormatNumberWithUserSettings()

  const preemptibleJobsCount = preemptibleJobs.length
  const selectedJobsCount = selectedJobs.length
  return (
    <Dialog open={true} onClose={onClose} fullWidth maxWidth="xl" onKeyDown={handleDialogKeyDown}>
      <DialogTitle>
        {isLoadingJobs
          ? "Preempt jobs"
          : `Preempt ${formatNumber(preemptibleJobsCount)} ${preemptibleJobsCount === 1 ? "job" : "jobs"}`}
      </DialogTitle>
      <DialogContent sx={{ display: "flex", flexDirection: "column" }}>
        <ErrorBoundary FallbackComponent={AlertErrorFallback}>
          {isLoadingJobs && (
            <div className={dialogStyles.loadingInfo}>
              Fetching info on selected jobs...
              <CircularProgress variant="indeterminate" />
            </div>
          )}

          {error && (
            <Alert severity="error" sx={{ marginBottom: "0.5em" }}>
              Failed to fetch jobs: {error}
            </Alert>
          )}

          {!isLoadingJobs && !error && (
            <>
              {preemptibleJobs.length > 0 && preemptibleJobs.length < selectedJobs.length && (
                <Alert severity="info" sx={{ marginBottom: "0.5em" }}>
                  {formatNumber(selectedJobsCount)} {selectedJobsCount === 1 ? "job is" : "jobs are"} selected, but only{" "}
                  {formatNumber(preemptibleJobsCount)} {preemptibleJobsCount === 1 ? "job is" : "jobs are"} in a
                  non-terminated state.
                </Alert>
              )}

              {preemptibleJobs.length === 0 && (
                <Alert severity="success">
                  All selected jobs are in a terminated state already, therefore there is nothing to preempt.
                </Alert>
              )}

              {preemptibleJobs.length > 0 && (
                <JobStatusTable
                  jobsToRender={jobsToRender}
                  jobStatus={jobIdsToPreemptResponses}
                  totalJobCount={preemptibleJobs.length}
                  additionalColumnsToDisplay={[
                    { displayName: "State", formatter: formatState },
                    { displayName: "Submitted Time", formatter: formatSubmittedTime },
                  ]}
                  showStatus={Object.keys(jobIdsToPreemptResponses).length > 0}
                />
              )}

              <TextField
                value={preemptReason ?? ""}
                autoFocus={true}
                label={"Reason for job preemption"}
                margin={"normal"}
                type={"text"}
                required
                onChange={handlePreemptReasonChange}
                sx={{ maxWidth: "250px" }}
                slotProps={{
                  htmlInput: { inputMode: "numeric", pattern: "[0-9]+" },
                }}
              />
            </>
          )}
        </ErrorBoundary>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Close</Button>
        <Button
          onClick={handleRefetch}
          disabled={isLoadingJobs || isPreempting}
          variant="outlined"
          endIcon={<Refresh />}
        >
          Refetch jobs
        </Button>
        <Button
          onClick={handlePreemptJobs}
          loading={isPreempting}
          disabled={isLoadingJobs || hasAttemptedPreempt || preemptibleJobs.length === 0 || preemptReason === undefined}
          variant="contained"
          endIcon={<Dangerous />}
        >
          Preempt {formatNumber(preemptibleJobsCount)} {preemptibleJobsCount === 1 ? "job" : "jobs"}
        </Button>
      </DialogActions>
    </Dialog>
  )
}
