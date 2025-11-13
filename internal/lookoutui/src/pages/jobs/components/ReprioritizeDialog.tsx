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

import { getUniqueJobsMatchingFilters } from "../../../common/jobsDialogUtils"
import { waitMs } from "../../../common/utils"
import { AlertErrorFallback } from "../../../components/AlertErrorFallback"
import { useFormatNumberWithUserSettings } from "../../../components/hooks/formatNumberWithUserSettings"
import { useFormatIsoTimestampWithUserSettings } from "../../../components/hooks/formatTimeWithUserSettings"
import { useCustomSnackbar } from "../../../components/hooks/useCustomSnackbar"
import { isTerminatedJobState, Job, JobFiltersWithExcludes, JobId } from "../../../models/lookoutModels"
import { useAuthenticatedFetch, useGetAccessToken } from "../../../oidcAuth"
import { IGetJobsService } from "../../../services/lookout/GetJobsService"
import { UpdateJobsService } from "../../../services/lookout/UpdateJobsService"

import dialogStyles from "./DialogStyles.module.css"
import { JobStatusTable } from "./JobStatusTable"

interface ReprioritizeDialogProps {
  onClose: () => void
  selectedItemFilters: JobFiltersWithExcludes[]
  getJobsService: IGetJobsService
  updateJobsService: UpdateJobsService
}

export const ReprioritizeDialog = ({
  onClose,
  selectedItemFilters,
  getJobsService,
  updateJobsService,
}: ReprioritizeDialogProps) => {
  const mounted = useRef(false)
  // State
  const [isLoadingJobs, setIsLoadingJobs] = useState(true)
  const [selectedJobs, setSelectedJobs] = useState<Job[]>([])
  const [jobIdsToReprioritizeResponses, setJobIdsToReprioritizeResponses] = useState<Record<JobId, string>>({})
  const reprioritizableJobs = useMemo(
    () => selectedJobs.filter((job) => !isTerminatedJobState(job.state)),
    [selectedJobs],
  )
  const [newPriority, setNewPriority] = useState<number | undefined>(undefined)
  const [isReprioritizing, setIsReprioritizing] = useState(false)
  const [hasAttemptedReprioritize, setHasAttemptedReprioritize] = useState(false)
  const openSnackbar = useCustomSnackbar()

  const formatIsoTimestamp = useFormatIsoTimestampWithUserSettings()

  const getAccessToken = useGetAccessToken()

  const authenticatedFetch = useAuthenticatedFetch()

  // Actions
  const fetchSelectedJobs = useCallback(async () => {
    if (!mounted.current) {
      return
    }

    setIsLoadingJobs(true)

    const uniqueJobsToReprioritize = await getUniqueJobsMatchingFilters(
      authenticatedFetch,
      selectedItemFilters,
      false,
      getJobsService,
    )
    const sortedJobs = _.orderBy(uniqueJobsToReprioritize, (job) => job.jobId, "desc")

    if (!mounted.current) {
      return
    }

    setSelectedJobs(sortedJobs)
    setIsLoadingJobs(false)
  }, [selectedItemFilters, getJobsService])

  const reprioritizeJobs = useCallback(async () => {
    if (newPriority === undefined) {
      return
    }

    setIsReprioritizing(true)

    const accessToken = await getAccessToken()
    const response = await updateJobsService.reprioritizeJobs(reprioritizableJobs, newPriority, accessToken)

    if (response.failedJobIds.length === 0) {
      openSnackbar(
        "Successfully changed priority. Jobs may take some time to reprioritize, but you may navigate away.",
        "success",
      )
    } else if (response.successfulJobIds.length === 0) {
      openSnackbar("All jobs failed to reprioritize. See table for error responses.", "error")
    } else {
      openSnackbar("Some jobs failed to reprioritize. See table for error responses.", "warning")
    }

    const newResponseStatus = { ...jobIdsToReprioritizeResponses }
    response.successfulJobIds.map((jobId) => (newResponseStatus[jobId] = "Success"))
    response.failedJobIds.map(({ jobId, errorReason }) => (newResponseStatus[jobId] = errorReason))

    setJobIdsToReprioritizeResponses(newResponseStatus)
    setHasAttemptedReprioritize(true)
    setIsReprioritizing(false)
  }, [newPriority, reprioritizableJobs, jobIdsToReprioritizeResponses])

  // On opening the dialog
  useEffect(() => {
    mounted.current = true
    // eslint-disable-next-line no-console
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
    setHasAttemptedReprioritize(false)
  }, [])

  const handleReprioritizeJobs = useCallback(async () => {
    await reprioritizeJobs()

    // Wait a short time period, then check the latest state
    setIsLoadingJobs(true)
    await waitMs(500)
    await fetchSelectedJobs()
  }, [reprioritizeJobs])

  const handleRefetch = useCallback(async () => {
    setIsLoadingJobs(true)
    setJobIdsToReprioritizeResponses({})
    setHasAttemptedReprioritize(false)
    await fetchSelectedJobs()
  }, [fetchSelectedJobs])

  const jobsToRender = useMemo(() => reprioritizableJobs.slice(0, 1000), [reprioritizableJobs])
  const formatPriority = useCallback((job: Job) => job.priority.toString(), [])
  const formatSubmittedTime = useCallback((job: Job) => formatIsoTimestamp(job.submitted, "full"), [formatIsoTimestamp])

  const formatNumber = useFormatNumberWithUserSettings()

  const reprioritizableJobsCount = reprioritizableJobs.length
  const selectedJobsCount = selectedJobs.length
  return (
    <Dialog open={true} onClose={onClose} fullWidth maxWidth="xl">
      <DialogTitle>
        {isLoadingJobs
          ? "Reprioritize jobs"
          : `Reprioritize ${formatNumber(reprioritizableJobsCount)} ${reprioritizableJobsCount === 1 ? "job" : "jobs"}`}
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
              {reprioritizableJobs.length > 0 && reprioritizableJobs.length < selectedJobs.length && (
                <Alert severity="info" sx={{ marginBottom: "0.5em" }}>
                  {formatNumber(selectedJobsCount)} {selectedJobsCount === 1 ? "job is" : "jobs are"} selected, but only{" "}
                  {formatNumber(reprioritizableJobsCount)} {reprioritizableJobsCount === 1 ? "job is" : "jobs are"} in a
                  non-terminated state.
                </Alert>
              )}

              {reprioritizableJobs.length === 0 && (
                <Alert severity="success">
                  All selected jobs are in a terminated state already, therefore there is nothing to reprioritize.
                </Alert>
              )}

              {reprioritizableJobs.length > 0 && (
                <JobStatusTable
                  jobsToRender={jobsToRender}
                  jobStatus={jobIdsToReprioritizeResponses}
                  totalJobCount={reprioritizableJobs.length}
                  additionalColumnsToDisplay={[
                    { displayName: "Priority", formatter: formatPriority },
                    { displayName: "Submitted Time", formatter: formatSubmittedTime },
                  ]}
                  showStatus={Object.keys(jobIdsToReprioritizeResponses).length > 0}
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
          disabled={isLoadingJobs || isReprioritizing}
          variant="outlined"
          endIcon={<Refresh />}
        >
          Refetch jobs
        </Button>
        <Button
          onClick={handleReprioritizeJobs}
          loading={isReprioritizing}
          disabled={
            isLoadingJobs || hasAttemptedReprioritize || reprioritizableJobs.length === 0 || newPriority === undefined
          }
          variant="contained"
          endIcon={<Dangerous />}
        >
          Reprioritize {formatNumber(reprioritizableJobsCount)} {reprioritizableJobsCount === 1 ? "job" : "jobs"}
        </Button>
      </DialogActions>
    </Dialog>
  )
}
