import { useCallback, useEffect, useMemo, useRef, useState } from "react"

import { Checkbox } from "@material-ui/core"
import { Refresh, Dangerous } from "@mui/icons-material"
import { LoadingButton } from "@mui/lab"
import { Button, CircularProgress, Dialog, DialogActions, DialogContent, DialogTitle, Alert } from "@mui/material"
import _ from "lodash"
import { isTerminatedJobState, Job, JobFilter, JobId } from "models/lookoutV2Models"
import { IGetJobsService } from "services/lookoutV2/GetJobsService"
import { UpdateJobsService } from "services/lookoutV2/UpdateJobsService"
import { pl, waitMillis, PlatformCancelReason } from "utils"
import { getUniqueJobsMatchingFilters } from "utils/jobsDialogUtils"
import { formatJobState } from "utils/jobsTableFormatters"

import dialogStyles from "./DialogStyles.module.css"
import { JobStatusTable } from "./JobStatusTable"
import { useCustomSnackbar } from "../../hooks/useCustomSnackbar"

interface CancelDialogProps {
  onClose: () => void
  selectedItemFilters: JobFilter[][]
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
    const response = await updateJobsService.cancelJobs(cancellableJobs, reason)

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
  const formatState = useCallback((job) => formatJobState(job.state), [])
  const formatSubmittedTime = useCallback((job) => job.submitted, [])
  return (
    <Dialog open={true} onClose={onClose} fullWidth maxWidth="xl">
      <DialogTitle>Cancel {isLoadingJobs ? "jobs" : pl(cancellableJobs, "job")}</DialogTitle>

      <DialogContent>
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
                {pl(selectedJobs.length, "job is", "jobs are")} selected, but only{" "}
                {pl(cancellableJobs.length, "job is", "jobs are")} in a cancellable (non-terminated) state.
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
        <LoadingButton
          onClick={handleCancelJobs}
          loading={isCancelling}
          disabled={isLoadingJobs || hasAttemptedCancel || cancellableJobs.length === 0}
          variant="contained"
          endIcon={<Dangerous />}
        >
          Cancel {isLoadingJobs ? "jobs" : pl(cancellableJobs, "job")}
        </LoadingButton>
      </DialogActions>
    </Dialog>
  )
}
