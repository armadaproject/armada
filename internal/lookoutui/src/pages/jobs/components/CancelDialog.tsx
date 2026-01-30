import { useCallback, useEffect, useMemo, useState } from "react"

import { Refresh, Dangerous } from "@mui/icons-material"
import {
  Checkbox,
  Button,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Alert,
} from "@mui/material"
import { ErrorBoundary } from "react-error-boundary"

import { formatJobState } from "../../../common/jobsTableFormatters"
import { waitMs, PlatformCancelReason } from "../../../common/utils"
import { AlertErrorFallback } from "../../../components/AlertErrorFallback"
import { useFormatNumberWithUserSettings } from "../../../components/hooks/formatNumberWithUserSettings"
import { useFormatIsoTimestampWithUserSettings } from "../../../components/hooks/formatTimeWithUserSettings"
import { useCustomSnackbar } from "../../../components/hooks/useCustomSnackbar"
import { isTerminatedJobState, Job, JobFiltersWithExcludes, JobId } from "../../../models/lookoutModels"
import { useCancelJobs } from "../../../services/lookout/useCancelJobs"
import { useGetAllJobsMatchingFilters } from "../../../services/lookout/useGetAllJobsMatchingFilters"

import dialogStyles from "./DialogStyles.module.css"
import { JobStatusTable } from "./JobStatusTable"

interface CancelDialogProps {
  onClose: () => void
  selectedItemFilters: JobFiltersWithExcludes[]
}

export const CancelDialog = ({ onClose, selectedItemFilters }: CancelDialogProps) => {
  // State
  const [jobIdsToCancelResponses, setJobIdsToCancelResponses] = useState<Record<JobId, string>>({})
  const [isCancelling, setIsCancelling] = useState(false)
  const [hasAttemptedCancel, setHasAttemptedCancel] = useState(false)
  const [isPlatformCancel, setIsPlatformCancel] = useState(false)
  const [refetchAfterCancel, setRefetchAfterCancel] = useState(false)
  const openSnackbar = useCustomSnackbar()

  const formatIsoTimestamp = useFormatIsoTimestampWithUserSettings()
  const cancelJobsMutation = useCancelJobs()

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

  const cancellableJobs = useMemo(() => selectedJobs.filter((job) => !isTerminatedJobState(job.state)), [selectedJobs])

  // Actions
  const cancelSelectedJobs = useCallback(async () => {
    setIsCancelling(true)

    const reason = isPlatformCancel ? PlatformCancelReason : ""

    try {
      const response = await cancelJobsMutation.mutateAsync({
        jobs: cancellableJobs,
        reason,
      })

      if (response.failedJobIds.length === 0) {
        openSnackbar(
          "Successfully began cancellation. Jobs may take some time to cancel, but you may navigate away.",
          "success",
        )
      } else if (response.successfulJobIds.length === 0) {
        openSnackbar("All jobs failed to cancel. See table for error responses.", "error")
      } else {
        openSnackbar("Some jobs failed to cancel. See table for error responses.", "warning")
      }

      const newResponseStatus = { ...jobIdsToCancelResponses }
      response.successfulJobIds.map((jobId) => (newResponseStatus[jobId] = "Success"))
      response.failedJobIds.map(({ jobId, errorReason }) => (newResponseStatus[jobId] = errorReason))

      setJobIdsToCancelResponses(newResponseStatus)
      setHasAttemptedCancel(true)
    } finally {
      setIsCancelling(false)
    }
  }, [cancellableJobs, jobIdsToCancelResponses, isPlatformCancel, cancelJobsMutation, openSnackbar])

  // Wait after cancel and refetch
  useEffect(() => {
    if (refetchAfterCancel) {
      const doRefetch = async () => {
        await waitMs(500)
        refetch()
        setRefetchAfterCancel(false)
      }
      doRefetch()
    }
  }, [refetchAfterCancel, refetch])

  // Event handlers
  const handleCancelJobs = useCallback(async () => {
    await cancelSelectedJobs()
    // Trigger a refetch after a small delay
    setRefetchAfterCancel(true)
  }, [cancelSelectedJobs])

  const handleRefetch = useCallback(() => {
    setJobIdsToCancelResponses({})
    setHasAttemptedCancel(false)
    refetch()
  }, [refetch])

  const handleDialogKeyDown = useCallback(
    (event: React.KeyboardEvent) => {
      if (
        event.key === "Enter" &&
        !isLoadingJobs &&
        !hasAttemptedCancel &&
        !isCancelling &&
        cancellableJobs.length > 0
      ) {
        event.preventDefault()
        handleCancelJobs()
      }
    },
    [isLoadingJobs, hasAttemptedCancel, isCancelling, cancellableJobs.length, handleCancelJobs],
  )

  const jobsToRender = useMemo(() => cancellableJobs.slice(0, 1000), [cancellableJobs])
  const formatState = useCallback((job: Job) => formatJobState(job.state), [])
  const formatSubmittedTime = useCallback((job: Job) => formatIsoTimestamp(job.submitted, "full"), [formatIsoTimestamp])

  const formatNumber = useFormatNumberWithUserSettings()

  const cancellableJobsCount = cancellableJobs.length
  const selectedJobsCount = selectedJobs.length
  return (
    <Dialog open={true} onClose={onClose} fullWidth maxWidth="xl" onKeyDown={handleDialogKeyDown}>
      <DialogTitle>
        {isLoadingJobs
          ? "Cancel jobs"
          : `Cancel ${formatNumber(cancellableJobsCount)} ${cancellableJobsCount === 1 ? "job" : "jobs"}`}
      </DialogTitle>

      <DialogContent>
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
              {cancellableJobs.length > 0 && cancellableJobs.length < selectedJobs.length && (
                <Alert severity="info" sx={{ marginBottom: "0.5em" }}>
                  {formatNumber(selectedJobsCount)} {selectedJobsCount === 1 ? "job is" : "jobs are"} selected, but only{" "}
                  {formatNumber(cancellableJobsCount)} {cancellableJobsCount === 1 ? "job is" : "jobs are"} in a
                  cancellable (non-terminated) state.
                </Alert>
              )}

              {cancellableJobs.length === 0 && (
                <Alert severity="success">
                  All selected jobs are in a terminated state already, therefore there is nothing to cancel.
                </Alert>
              )}

              {cancellableJobs.length > 0 && (
                <JobStatusTable
                  jobsToRender={jobsToRender}
                  jobStatus={jobIdsToCancelResponses}
                  totalJobCount={cancellableJobs.length}
                  additionalColumnsToDisplay={[
                    { displayName: "State", formatter: formatState },
                    { displayName: "Submitted Time", formatter: formatSubmittedTime },
                  ]}
                  showStatus={Object.keys(jobIdsToCancelResponses).length > 0}
                />
              )}
            </>
          )}
        </ErrorBoundary>
      </DialogContent>

      <DialogActions>
        <Button onClick={onClose}>Close</Button>
        <div
          style={{
            display: "flex",
            flexDirection: "row",
            alignItems: "center",
            padding: "0 10px 0 10px",
          }}
        >
          <Checkbox
            style={{
              padding: "3px",
            }}
            checked={isPlatformCancel}
            disabled={isLoadingJobs || hasAttemptedCancel || cancellableJobs.length === 0}
            onChange={(event) => setIsPlatformCancel(event.target.checked)}
          />
          <label>Platform error</label>
        </div>
        <Button
          onClick={handleRefetch}
          disabled={isLoadingJobs || isCancelling}
          variant="outlined"
          endIcon={<Refresh />}
        >
          Refetch jobs
        </Button>
        <Button
          onClick={handleCancelJobs}
          loading={isCancelling}
          disabled={isLoadingJobs || hasAttemptedCancel || cancellableJobs.length === 0}
          variant="contained"
          endIcon={<Dangerous />}
        >
          Cancel {formatNumber(cancellableJobsCount)} {cancellableJobsCount === 1 ? "job" : "jobs"}
        </Button>
      </DialogActions>
    </Dialog>
  )
}
