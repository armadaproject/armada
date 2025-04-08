import { ChangeEvent, useCallback, useEffect, useMemo, useRef, useState } from "react"

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
import { waitMillis } from "../../utils"
import { getUniqueJobsMatchingFilters } from "../../utils/jobsDialogUtils"
import { AlertErrorFallback } from "../AlertErrorFallback"

interface ReprioritiseDialogProps {
  onClose: () => void
  selectedItemFilters: JobFiltersWithExcludes[]
  getJobsService: IGetJobsService
  updateJobsService: UpdateJobsService
}

export const ReprioritiseDialog = ({
  onClose,
  selectedItemFilters,
  getJobsService,
  updateJobsService,
}: ReprioritiseDialogProps) => {
  const mounted = useRef(false)
  // State
  const [isLoadingJobs, setIsLoadingJobs] = useState(true)
  const [selectedJobs, setSelectedJobs] = useState<Job[]>([])
  const [jobIdsToReprioritiseResponses, setJobIdsToReprioritiseResponses] = useState<Record<JobId, string>>({})
  const reprioritisableJobs = useMemo(
    () => selectedJobs.filter((job) => !isTerminatedJobState(job.state)),
    [selectedJobs],
  )
  const [newPriority, setNewPriority] = useState<number | undefined>(undefined)
  const [isReprioritising, setIsReprioritising] = useState(false)
  const [hasAttemptedReprioritise, setHasAttemptedReprioritise] = useState(false)
  const openSnackbar = useCustomSnackbar()

  const formatIsoTimestamp = useFormatIsoTimestampWithUserSettings()

  const getAccessToken = useGetAccessToken()

  // Actions
  const fetchSelectedJobs = useCallback(async () => {
    if (!mounted.current) {
      return
    }

    setIsLoadingJobs(true)

    const uniqueJobsToReprioritise = await getUniqueJobsMatchingFilters(selectedItemFilters, false, getJobsService)
    const sortedJobs = _.orderBy(uniqueJobsToReprioritise, (job) => job.jobId, "desc")

    if (!mounted.current) {
      return
    }

    setSelectedJobs(sortedJobs)
    setIsLoadingJobs(false)
  }, [selectedItemFilters, getJobsService])

  const reprioritiseJobs = useCallback(async () => {
    if (newPriority === undefined) {
      return
    }

    setIsReprioritising(true)

    const accessToken = await getAccessToken()
    const response = await updateJobsService.reprioritiseJobs(reprioritisableJobs, newPriority, accessToken)

    if (response.failedJobIds.length === 0) {
      openSnackbar(
        "Successfully changed priority. Jobs may take some time to reprioritise, but you may navigate away.",
        "success",
      )
    } else if (response.successfulJobIds.length === 0) {
      openSnackbar("All jobs failed to reprioritise. See table for error responses.", "error")
    } else {
      openSnackbar("Some jobs failed to reprioritise. See table for error responses.", "warning")
    }

    const newResponseStatus = { ...jobIdsToReprioritiseResponses }
    response.successfulJobIds.map((jobId) => (newResponseStatus[jobId] = "Success"))
    response.failedJobIds.map(({ jobId, errorReason }) => (newResponseStatus[jobId] = errorReason))

    setJobIdsToReprioritiseResponses(newResponseStatus)
    setHasAttemptedReprioritise(true)
    setIsReprioritising(false)
  }, [newPriority, reprioritisableJobs, jobIdsToReprioritiseResponses])

  // On opening the dialog
  useEffect(() => {
    mounted.current = true
    fetchSelectedJobs().catch(console.error)
    return () => {
      mounted.current = false
    }
  }, [])

  // Event handlers
  const handlePriorityChange = useCallback((event: ChangeEvent<HTMLInputElement>) => {
    const val = event.target.value
    const num = Number(event.target.value)
    if (val.length > 0 && !Number.isNaN(num)) {
      setNewPriority(num)
    } else {
      setNewPriority(undefined)
    }
    setHasAttemptedReprioritise(false)
  }, [])

  const handleReprioritiseJobs = useCallback(async () => {
    await reprioritiseJobs()

    // Wait a short time period, then check the latest state
    setIsLoadingJobs(true)
    await waitMillis(500)
    await fetchSelectedJobs()
  }, [reprioritiseJobs])

  const handleRefetch = useCallback(async () => {
    setIsLoadingJobs(true)
    setJobIdsToReprioritiseResponses({})
    setHasAttemptedReprioritise(false)
    await fetchSelectedJobs()
  }, [fetchSelectedJobs])

  const jobsToRender = useMemo(() => reprioritisableJobs.slice(0, 1000), [reprioritisableJobs])
  const formatPriority = useCallback((job: Job) => job.priority.toString(), [])
  const formatSubmittedTime = useCallback((job: Job) => formatIsoTimestamp(job.submitted, "full"), [formatIsoTimestamp])

  const formatNumber = useFormatNumberWithUserSettings()

  const reprioritisableJobsCount = reprioritisableJobs.length
  const selectedJobsCount = selectedJobs.length
  return (
    <Dialog open={true} onClose={onClose} fullWidth maxWidth="xl">
      <DialogTitle>
        {isLoadingJobs
          ? "Reprioritise jobs"
          : `Reprioritise ${formatNumber(reprioritisableJobsCount)} ${reprioritisableJobsCount === 1 ? "job" : "jobs"}`}
      </DialogTitle>
      <DialogContent sx={{ display: "flex", flexDirection: "column" }}>
        <ErrorBoundary FallbackComponent={AlertErrorFallback}>
          {isLoadingJobs && (
            <div className={dialogStyles.loadingInfo}>
              Fetching info on selected jobs...
              <CircularProgress variant="indeterminate" />
            </div>
          )}

          {!isLoadingJobs && (
            <>
              {reprioritisableJobs.length > 0 && reprioritisableJobs.length < selectedJobs.length && (
                <Alert severity="info" sx={{ marginBottom: "0.5em" }}>
                  {formatNumber(selectedJobsCount)} {selectedJobsCount === 1 ? "job is" : "jobs are"} selected, but only{" "}
                  {formatNumber(reprioritisableJobsCount)} {reprioritisableJobsCount === 1 ? "job is" : "jobs are"} in a
                  non-terminated state.
                </Alert>
              )}

              {reprioritisableJobs.length === 0 && (
                <Alert severity="success">
                  All selected jobs are in a terminated state already, therefore there is nothing to reprioritise.
                </Alert>
              )}

              {reprioritisableJobs.length > 0 && (
                <JobStatusTable
                  jobsToRender={jobsToRender}
                  jobStatus={jobIdsToReprioritiseResponses}
                  totalJobCount={reprioritisableJobs.length}
                  additionalColumnsToDisplay={[
                    { displayName: "Priority", formatter: formatPriority },
                    { displayName: "Submitted Time", formatter: formatSubmittedTime },
                  ]}
                  showStatus={Object.keys(jobIdsToReprioritiseResponses).length > 0}
                />
              )}

              <TextField
                value={newPriority ?? ""}
                autoFocus={true}
                label={"New priority for jobs"}
                helperText="(0 = highest priority)"
                margin={"normal"}
                type={"text"}
                required
                onChange={handlePriorityChange}
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
          disabled={isLoadingJobs || isReprioritising}
          variant="outlined"
          endIcon={<Refresh />}
        >
          Refetch jobs
        </Button>
        <Button
          onClick={handleReprioritiseJobs}
          loading={isReprioritising}
          disabled={
            isLoadingJobs || hasAttemptedReprioritise || reprioritisableJobs.length === 0 || newPriority === undefined
          }
          variant="contained"
          endIcon={<Dangerous />}
        >
          Reprioritise {formatNumber(reprioritisableJobsCount)} {reprioritisableJobsCount === 1 ? "job" : "jobs"}
        </Button>
      </DialogActions>
    </Dialog>
  )
}
