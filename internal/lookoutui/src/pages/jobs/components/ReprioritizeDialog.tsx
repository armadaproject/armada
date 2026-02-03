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

import { waitMs } from "../../../common/utils"
import { AlertErrorFallback } from "../../../components/AlertErrorFallback"
import { useFormatNumberWithUserSettings } from "../../../components/hooks/formatNumberWithUserSettings"
import { useFormatIsoTimestampWithUserSettings } from "../../../components/hooks/formatTimeWithUserSettings"
import { useCustomSnackbar } from "../../../components/hooks/useCustomSnackbar"
import { isTerminatedJobState, Job, JobFiltersWithExcludes, JobId } from "../../../models/lookoutModels"
import { useGetAllJobsMatchingFilters } from "../../../services/lookout/useGetAllJobsMatchingFilters"
import { useReprioritizeJobs } from "../../../services/lookout/useReprioritizeJobs"

import dialogStyles from "./DialogStyles.module.css"
import { JobStatusTable } from "./JobStatusTable"

interface ReprioritizeDialogProps {
  onClose: () => void
  selectedItemFilters: JobFiltersWithExcludes[]
}

export const ReprioritizeDialog = ({ onClose, selectedItemFilters }: ReprioritizeDialogProps) => {
  // State
  const [jobIdsToReprioritizeResponses, setJobIdsToReprioritizeResponses] = useState<Record<JobId, string>>({})
  const [newPriority, setNewPriority] = useState<number | undefined>(undefined)
  const [isReprioritizing, setIsReprioritizing] = useState(false)
  const [hasAttemptedReprioritize, setHasAttemptedReprioritize] = useState(false)
  const [refetchAfterReprioritize, setRefetchAfterReprioritize] = useState(false)
  const openSnackbar = useCustomSnackbar()

  const formatIsoTimestamp = useFormatIsoTimestampWithUserSettings()
  const reprioritizeJobsMutation = useReprioritizeJobs()

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

  const reprioritizableJobs = useMemo(
    () => selectedJobs.filter((job) => !isTerminatedJobState(job.state)),
    [selectedJobs],
  )

  // Actions
  const reprioritizeJobs = useCallback(async () => {
    if (newPriority === undefined) {
      return
    }

    setIsReprioritizing(true)

    try {
      const response = await reprioritizeJobsMutation.mutateAsync({
        jobs: reprioritizableJobs,
        newPriority,
      })

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
    } finally {
      setIsReprioritizing(false)
    }
  }, [newPriority, reprioritizableJobs, jobIdsToReprioritizeResponses, reprioritizeJobsMutation, openSnackbar])

  // Wait after reprioritize and refetch
  useEffect(() => {
    if (refetchAfterReprioritize) {
      const doRefetch = async () => {
        await waitMs(500)
        refetch()
        setRefetchAfterReprioritize(false)
      }
      doRefetch()
    }
  }, [refetchAfterReprioritize, refetch])

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
    // Trigger a refetch after a small delay
    setRefetchAfterReprioritize(true)
  }, [reprioritizeJobs])

  const handleRefetch = useCallback(() => {
    setJobIdsToReprioritizeResponses({})
    setHasAttemptedReprioritize(false)
    refetch()
  }, [refetch])

  const handleSubmit = useCallback(
    (event: React.FormEvent) => {
      event.preventDefault()
      handleReprioritizeJobs()
    },
    [handleReprioritizeJobs],
  )

  const handleDialogKeyDown = useCallback(
    (event: React.KeyboardEvent) => {
      if (
        event.key === "Enter" &&
        !isLoadingJobs &&
        !hasAttemptedReprioritize &&
        !isReprioritizing &&
        reprioritizableJobs.length > 0 &&
        newPriority !== undefined
      ) {
        handleSubmit(event as React.FormEvent)
      }
    },
    [isLoadingJobs, hasAttemptedReprioritize, isReprioritizing, reprioritizableJobs.length, newPriority, handleSubmit],
  )

  const jobsToRender = useMemo(() => reprioritizableJobs.slice(0, 1000), [reprioritizableJobs])
  const formatPriority = useCallback((job: Job) => job.priority.toString(), [])
  const formatSubmittedTime = useCallback((job: Job) => formatIsoTimestamp(job.submitted, "full"), [formatIsoTimestamp])

  const formatNumber = useFormatNumberWithUserSettings()

  const reprioritizableJobsCount = reprioritizableJobs.length
  const selectedJobsCount = selectedJobs.length
  return (
    <Dialog open={true} onClose={onClose} fullWidth maxWidth="xl" onKeyDown={handleDialogKeyDown}>
      <DialogTitle>
        {isLoadingJobs
          ? "Reprioritize jobs"
          : `Reprioritize ${formatNumber(reprioritizableJobsCount)} ${reprioritizableJobsCount === 1 ? "job" : "jobs"}`}
      </DialogTitle>
      <DialogContent sx={{ display: "flex", flexDirection: "column" }}>
        <ErrorBoundary FallbackComponent={AlertErrorFallback}>
          <form id="reprioritize-form" onSubmit={handleSubmit}>
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
                {reprioritizableJobs.length > 0 && reprioritizableJobs.length < selectedJobs.length && (
                  <Alert severity="info" sx={{ marginBottom: "0.5em" }}>
                    {formatNumber(selectedJobsCount)} {selectedJobsCount === 1 ? "job is" : "jobs are"} selected, but
                    only {formatNumber(reprioritizableJobsCount)}{" "}
                    {reprioritizableJobsCount === 1 ? "job is" : "jobs are"} in a non-terminated state.
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
          </form>
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
          type="submit"
          form="reprioritize-form"
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
