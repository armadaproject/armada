import { ChangeEvent, useCallback, useEffect, useMemo, useRef, useState } from "react"

import { Refresh, Dangerous } from "@mui/icons-material"
import { LoadingButton } from "@mui/lab"
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
import { isTerminatedJobState, Job, JobFilter, JobId } from "models/lookoutV2Models"
import { IGetJobsService } from "services/lookoutV2/GetJobsService"
import { UpdateJobsService } from "services/lookoutV2/UpdateJobsService"
import { pl, waitMillis } from "utils"
import { getUniqueJobsMatchingFilters } from "utils/jobsDialogUtils"

import dialogStyles from "./DialogStyles.module.css"
import { JobStatusTable } from "./JobStatusTable"
import { getAccessToken, useUserManager } from "../../auth"
import { useCustomSnackbar } from "../../hooks/useCustomSnackbar"

interface ReprioritiseDialogProps {
  onClose: () => void
  selectedItemFilters: JobFilter[][]
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

  const userManager = useUserManager()

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

    const accessToken = userManager && (await getAccessToken(userManager))
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
  const formatPriority = useCallback((job) => job.priority, [])
  const formatSubmittedTime = useCallback((job) => job.submitted, [])
  return (
    <Dialog open={true} onClose={onClose} fullWidth maxWidth="xl">
      <DialogTitle>Reprioritise {isLoadingJobs ? "jobs" : pl(reprioritisableJobs, "job")}</DialogTitle>

      <DialogContent sx={{ display: "flex", flexDirection: "column" }}>
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
                {pl(selectedJobs.length, "job is", "jobs are")} selected, but only{" "}
                {pl(reprioritisableJobs, "job is", "jobs are")} in a non-terminated state.
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
              inputProps={{ inputMode: "numeric", pattern: "[0-9]+" }}
              onChange={handlePriorityChange}
              sx={{ maxWidth: "250px" }}
            />
          </>
        )}
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
        <LoadingButton
          onClick={handleReprioritiseJobs}
          loading={isReprioritising}
          disabled={
            isLoadingJobs || hasAttemptedReprioritise || reprioritisableJobs.length === 0 || newPriority === undefined
          }
          variant="contained"
          endIcon={<Dangerous />}
        >
          Reprioritise {isLoadingJobs ? "jobs" : pl(reprioritisableJobs, "job")}
        </LoadingButton>
      </DialogActions>
    </Dialog>
  )
}
