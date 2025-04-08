import { useCallback, useEffect, useMemo, useRef, useState } from "react"

import { Refresh, Dangerous } from "@mui/icons-material"
import { Checkbox } from "@mui/material"
import { Button, CircularProgress, Dialog, DialogActions, DialogContent, DialogTitle, Alert } from "@mui/material"
import _ from "lodash"
import { ErrorBoundary } from "react-error-boundary"

import dialogStyles from "./DialogStyles.module.css"
import { JobStatusTable } from "./JobStatusTable"
import { useFormatNumberWithUserSettings } from "../../hooks/formatNumberWithUserSettings"
import { useFormatIsoTimestampWithUserSettings } from "../../hooks/formatTimeWithUserSettings"
import { useCustomSnackbar } from "../../hooks/useCustomSnackbar"
import { isTerminatedJobState, Job, JobFiltersWithExcludes, JobId } from "../../models/lookoutModels"
import { useGetAccessToken } from "../../oidcAuth"
import { IGetJobsService } from "../../services/lookout/GetJobsService"
import { UpdateJobsService } from "../../services/lookout/UpdateJobsService"
import { waitMillis, PlatformCancelReason } from "../../utils"
import { getUniqueJobsMatchingFilters } from "../../utils/jobsDialogUtils"
import { formatJobState } from "../../utils/jobsTableFormatters"
import { AlertErrorFallback } from "../AlertErrorFallback"

interface CancelDialogProps {
  onClose: () => void
  selectedItemFilters: JobFiltersWithExcludes[]
  getJobsService: IGetJobsService
  updateJobsService: UpdateJobsService
}

export const CancelDialog = ({
  onClose,
  selectedItemFilters,
  getJobsService,
  updateJobsService,
}: CancelDialogProps) => {
  const mounted = useRef(false)
  // State
  const [isLoadingJobs, setIsLoadingJobs] = useState(true)
  const [selectedJobs, setSelectedJobs] = useState<Job[]>([])
  const [jobIdsToCancelResponses, setJobIdsToCancelResponses] = useState<Record<JobId, string>>({})
  const cancellableJobs = useMemo(() => selectedJobs.filter((job) => !isTerminatedJobState(job.state)), [selectedJobs])
  const [isCancelling, setIsCancelling] = useState(false)
  const [hasAttemptedCancel, setHasAttemptedCancel] = useState(false)
  const [isPlatformCancel, setIsPlatformCancel] = useState(false)
  const openSnackbar = useCustomSnackbar()

  const formatIsoTimestamp = useFormatIsoTimestampWithUserSettings()

  const getAccessToken = useGetAccessToken()

  // Actions
  const fetchSelectedJobs = useCallback(async () => {
    if (!mounted.current) {
      return
    }

    setIsLoadingJobs(true)

    const uniqueJobsToCancel = await getUniqueJobsMatchingFilters(selectedItemFilters, false, getJobsService)
    const sortedJobs = _.orderBy(uniqueJobsToCancel, (job) => job.jobId, "desc")

    if (!mounted.current) {
      return
    }

    setSelectedJobs(sortedJobs)
    setIsLoadingJobs(false)
    setHasAttemptedCancel(false)
  }, [selectedItemFilters, getJobsService])

  const cancelSelectedJobs = useCallback(async () => {
    setIsCancelling(true)

    const reason = isPlatformCancel ? PlatformCancelReason : ""
    const accessToken = await getAccessToken()
    const response = await updateJobsService.cancelJobs(cancellableJobs, reason, accessToken)

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
    setIsCancelling(false)
    setHasAttemptedCancel(true)
  }, [cancellableJobs, jobIdsToCancelResponses, isPlatformCancel])

  // On dialog open
  useEffect(() => {
    mounted.current = true
    fetchSelectedJobs().catch(console.error)
    return () => {
      mounted.current = false
    }
  }, [])

  // Event handlers
  const handleCancelJobs = useCallback(async () => {
    await cancelSelectedJobs()

    // Wait a small period and then retrieve the job state of the cancelled jobs
    setIsLoadingJobs(true)
    await waitMillis(500)
    await fetchSelectedJobs()
  }, [cancelSelectedJobs, fetchSelectedJobs])

  const handleRefetch = useCallback(() => {
    setJobIdsToCancelResponses({})
    fetchSelectedJobs().catch(console.error)
  }, [fetchSelectedJobs])

  const jobsToRender = useMemo(() => cancellableJobs.slice(0, 1000), [cancellableJobs])
  const formatState = useCallback((job: Job) => formatJobState(job.state), [])
  const formatSubmittedTime = useCallback((job: Job) => formatIsoTimestamp(job.submitted, "full"), [formatIsoTimestamp])

  const formatNumber = useFormatNumberWithUserSettings()

  const cancellableJobsCount = cancellableJobs.length
  const selectedJobsCount = selectedJobs.length
  return (
    <Dialog open={true} onClose={onClose} fullWidth maxWidth="xl">
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

          {!isLoadingJobs && (
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
