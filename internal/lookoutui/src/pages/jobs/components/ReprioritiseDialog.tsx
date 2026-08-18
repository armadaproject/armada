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

import { Analytics, ANALYTICS_EVENTS } from "../../../analytics"
import { waitMs } from "../../../common/utils"
import { AlertErrorFallback } from "../../../components/AlertErrorFallback"
import { useFormatNumberWithUserSettings } from "../../../components/hooks/formatNumberWithUserSettings"
import { useFormatIsoTimestampWithUserSettings } from "../../../components/hooks/formatTimeWithUserSettings"
import { useCustomSnackbar } from "../../../components/hooks/useCustomSnackbar"
import { isTerminatedJobState, Job, JobFiltersWithExcludes, JobId } from "../../../models/lookoutModels"
import { useGetAllJobsMatchingFilters } from "../../../services/lookout/useGetAllJobsMatchingFilters"
import { useReprioritiseJobs } from "../../../services/lookout/useReprioritiseJobs"

import dialogStyles from "./DialogStyles.module.css"
import { JobStatusTable } from "./JobStatusTable"

interface ReprioritiseDialogProps {
  onClose: () => void
  selectedItemFilters: JobFiltersWithExcludes[]
}

export const ReprioritiseDialog = ({ onClose, selectedItemFilters }: ReprioritiseDialogProps) => {
  // State
  const [jobIdsToReprioritiseResponses, setJobIdsToReprioritiseResponses] = useState<Record<JobId, string>>({})
  const [newPriority, setNewPriority] = useState<number | undefined>(undefined)
  const [isReprioritising, setIsReprioritising] = useState(false)
  const [hasAttemptedReprioritise, setHasAttemptedReprioritise] = useState(false)
  const [refetchAfterReprioritise, setRefetchAfterReprioritise] = useState(false)
  const openSnackbar = useCustomSnackbar()

  const formatIsoTimestamp = useFormatIsoTimestampWithUserSettings()
  const reprioritiseJobsMutation = useReprioritiseJobs()

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

  const reprioritisableJobs = useMemo(
    () => selectedJobs.filter((job) => !isTerminatedJobState(job.state)),
    [selectedJobs],
  )

  // Actions
  const reprioritiseJobs = useCallback(async () => {
    if (newPriority === undefined) {
      return
    }

    setIsReprioritising(true)

    try {
      const response = await reprioritiseJobsMutation.mutateAsync({
        jobs: reprioritisableJobs,
        newPriority,
      })

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
    } finally {
      setIsReprioritising(false)
    }
  }, [newPriority, reprioritisableJobs, jobIdsToReprioritiseResponses, reprioritiseJobsMutation, openSnackbar])

  // Wait after reprioritise and refetch
  useEffect(() => {
    if (refetchAfterReprioritise) {
      const doRefetch = async () => {
        await waitMs(500)
        refetch()
        setRefetchAfterReprioritise(false)
      }
      doRefetch()
    }
  }, [refetchAfterReprioritise, refetch])

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
    // Trigger a refetch after a small delay
    setRefetchAfterReprioritise(true)
  }, [reprioritiseJobs])

  const handleRefetch = useCallback(() => {
    setJobIdsToReprioritiseResponses({})
    setHasAttemptedReprioritise(false)
    refetch()
  }, [refetch])

  const handleSubmit = useCallback(
    (event: React.FormEvent) => {
      event.preventDefault()
      handleReprioritiseJobs()
    },
    [handleReprioritiseJobs],
  )

  const handleDialogKeyDown = useCallback(
    (event: React.KeyboardEvent) => {
      if (
        event.key === "Enter" &&
        !isLoadingJobs &&
        !hasAttemptedReprioritise &&
        !isReprioritising &&
        reprioritisableJobs.length > 0 &&
        newPriority !== undefined
      ) {
        handleSubmit(event as React.FormEvent)
      }
    },
    [isLoadingJobs, hasAttemptedReprioritise, isReprioritising, reprioritisableJobs.length, newPriority, handleSubmit],
  )

  const jobsToRender = useMemo(() => reprioritisableJobs.slice(0, 1000), [reprioritisableJobs])
  const formatPriority = useCallback((job: Job) => job.priority.toString(), [])
  const formatSubmittedTime = useCallback((job: Job) => formatIsoTimestamp(job.submitted, "full"), [formatIsoTimestamp])

  const formatNumber = useFormatNumberWithUserSettings()

  const reprioritisableJobsCount = reprioritisableJobs.length
  const selectedJobsCount = selectedJobs.length
  return (
    <Dialog open={true} onClose={onClose} fullWidth maxWidth="xl" onKeyDown={handleDialogKeyDown}>
      <DialogTitle>
        {isLoadingJobs
          ? "Reprioritise jobs"
          : `Reprioritise ${formatNumber(reprioritisableJobsCount)} ${reprioritisableJobsCount === 1 ? "job" : "jobs"}`}
      </DialogTitle>
      <DialogContent sx={{ display: "flex", flexDirection: "column" }}>
        <ErrorBoundary FallbackComponent={AlertErrorFallback}>
          <form id="reprioritise-form" onSubmit={handleSubmit}>
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
                {reprioritisableJobs.length > 0 && reprioritisableJobs.length < selectedJobs.length && (
                  <Alert severity="info" sx={{ marginBottom: "0.5em" }}>
                    {formatNumber(selectedJobsCount)} {selectedJobsCount === 1 ? "job is" : "jobs are"} selected, but
                    only {formatNumber(reprioritisableJobsCount)}{" "}
                    {reprioritisableJobsCount === 1 ? "job is" : "jobs are"} in a non-terminated state.
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
          </form>
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
        <Analytics
          component={Button}
          eventName={ANALYTICS_EVENTS.REPRIORITISE_JOBS_CLICKED}
          eventData={{ newPriority: newPriority?.toString() || "" }}
          type="submit"
          form="reprioritise-form"
          loading={isReprioritising}
          disabled={
            isLoadingJobs || hasAttemptedReprioritise || reprioritisableJobs.length === 0 || newPriority === undefined
          }
          variant="contained"
          endIcon={<Dangerous />}
        >
          Reprioritise {formatNumber(reprioritisableJobsCount)} {reprioritisableJobsCount === 1 ? "job" : "jobs"}
        </Analytics>
      </DialogActions>
    </Dialog>
  )
}
