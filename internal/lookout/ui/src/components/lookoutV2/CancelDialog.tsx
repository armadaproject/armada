import { memo, useCallback, useEffect, useMemo, useState } from "react"

import { Refresh, Dangerous } from "@mui/icons-material"
import { LoadingButton } from "@mui/lab"
import {
  Button,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  TableRow,
  TableBody,
  TableCell,
  TableHead,
  Table,
  Alert,
} from "@mui/material"
import _ from "lodash"
import { formatJobState, isTerminatedJobState, Job, JobFilter, JobId } from "models/lookoutV2Models"
import { useSnackbar } from "notistack"
import { CancelJobsService } from "services/lookoutV2/CancelJobsService"
import { IGetJobsService } from "services/lookoutV2/GetJobsService"
import { pl } from "utils"

import styles from "./CancelDialog.module.css"

const MAX_JOBS_PER_REQUEST = 1000

const getAllJobsMatchingFilters = async (filters: JobFilter[], getJobsService: IGetJobsService): Promise<Job[]> => {
  const receivedJobs: Job[] = []
  let continuePaginating = true
  while (continuePaginating) {
    const { jobs, count: totalJobs } = await getJobsService.getJobs(
      filters,
      { direction: "DESC", field: "jobId" },
      receivedJobs.length,
      MAX_JOBS_PER_REQUEST,
      undefined,
    )

    receivedJobs.push(...jobs)

    if (receivedJobs.length >= totalJobs || jobs.length === 0) {
      continuePaginating = false
    }
  }

  return receivedJobs
}

interface CancelDialogProps {
  onClose: () => void
  selectedItemFilters: JobFilter[][]
  getJobsService: IGetJobsService
  cancelJobsService: CancelJobsService
}
export const CancelDialog = ({
  onClose,
  selectedItemFilters,
  getJobsService,
  cancelJobsService,
}: CancelDialogProps) => {
  const [isLoadingJobs, setIsLoadingJobs] = useState(true)
  const [isCancelling, setIsCancelling] = useState(false)
  const [hasAttemptedCancel, setHasAttemptedCancel] = useState(false)
  const [selectedJobs, setSelectedJobs] = useState<Job[]>([])
  const [jobIdsToCancelResponses, setJobIdsToCancelResponses] = useState<Record<JobId, string>>({})
  const { enqueueSnackbar } = useSnackbar()

  const cancellableJobs = useMemo(() => selectedJobs.filter((job) => !isTerminatedJobState(job.state)), [selectedJobs])
  useEffect(() => {
    if (cancellableJobs.length > 0) {
      setHasAttemptedCancel(false)
    }
  }, [cancellableJobs])

  const fetchSelectedJobs = useCallback(async () => {
    const jobsBySelectedItem = await Promise.all(
      selectedItemFilters.map(async (filters) => await getAllJobsMatchingFilters(filters, getJobsService)),
    )
    const uniqueJobsToCancel = _.uniqBy(jobsBySelectedItem.flat(), (job) => job.jobId)
    const sortedJobs = _.orderBy(uniqueJobsToCancel, (job) => job.jobId, "desc")

    setSelectedJobs(sortedJobs)
    setIsLoadingJobs(false)
  }, [selectedItemFilters, getJobsService])

  useEffect(() => {
    fetchSelectedJobs().catch(console.error)
  }, [fetchSelectedJobs])

  useEffect(() => {
    async function cancelJobs() {
      if (!isCancelling) {
        return
      }

      const jobIdsToCancel = cancellableJobs.map((job) => job.jobId)
      const response = await cancelJobsService.cancelJobs(jobIdsToCancel)

      if (response.failedJobIds.length === 0) {
        enqueueSnackbar(
          "Successfully began cancellation. Jobs may take some time to cancel, but you may navigate away.",
          { variant: "success" },
        )
      } else if (response.successfulJobIds.length === 0) {
        enqueueSnackbar("All jobs failed to cancel. See table for error responses.", { variant: "error" })
      } else {
        enqueueSnackbar("Some jobs failed to cancel. See table for error responses.", { variant: "warning" })
      }

      const newResponseStatus = { ...jobIdsToCancelResponses }
      response.successfulJobIds.map((jobId) => (newResponseStatus[jobId] = "Success"))
      response.failedJobIds.map(({ jobId, errorReason }) => (newResponseStatus[jobId] = errorReason))

      setJobIdsToCancelResponses(newResponseStatus)
      setIsCancelling(false)
      setHasAttemptedCancel(true)
    }

    cancelJobs().catch(console.error)
  }, [isCancelling])

  const handleCancelJobs = useCallback(async () => {
    setIsCancelling(true)
  }, [])

  const handleRefetch = () => {
    setIsLoadingJobs(true)
    setJobIdsToCancelResponses({})
    fetchSelectedJobs().catch(console.error)
  }

  const jobsToRender = useMemo(() => {
    return cancellableJobs.slice(0, 1000).map((job) => ({
      job,
      lastResponseStatus: jobIdsToCancelResponses[job.jobId],
    }))
  }, [cancellableJobs, jobIdsToCancelResponses])
  return (
    <Dialog open={true} onClose={onClose} fullWidth maxWidth="xl">
      <DialogTitle>Cancel {isLoadingJobs ? "jobs" : pl(cancellableJobs, "job")}</DialogTitle>
      <CancelDialogBody
        isLoading={isLoadingJobs}
        cancellableJobsToRenderWithStatus={jobsToRender}
        cancellableJobCount={cancellableJobs.length}
        selectedJobCount={selectedJobs.length}
      />
      <DialogActions>
        <Button onClick={onClose}>Close</Button>
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

interface CancelDialogBodyProps {
  isLoading: boolean
  cancellableJobsToRenderWithStatus: { job: Job; lastResponseStatus: string | undefined }[]
  cancellableJobCount: number
  selectedJobCount: number
}
const CancelDialogBody = memo(
  ({ isLoading, cancellableJobsToRenderWithStatus, cancellableJobCount, selectedJobCount }: CancelDialogBodyProps) => {
    const showResponseCol = cancellableJobsToRenderWithStatus.some(
      ({ lastResponseStatus }) => lastResponseStatus !== undefined,
    )
    return (
      <DialogContent>
        {isLoading && (
          <div className={styles.loadingInfo}>
            Fetching info on selected jobs...
            <CircularProgress variant="indeterminate" />
          </div>
        )}

        {!isLoading && (
          <>
            {cancellableJobCount > 0 && cancellableJobCount < selectedJobCount && (
              <Alert severity="info" sx={{ marginBottom: "0.5em" }}>
                {pl(selectedJobCount, "job is", "jobs are")} selected, but only{" "}
                {pl(cancellableJobCount, "job is", "jobs are")} in a cancellable (non-terminated) state.
              </Alert>
            )}

            {cancellableJobCount === 0 && (
              <Alert severity="success">
                All selected jobs are in a terminated state already, therefore there is nothing to cancel.
              </Alert>
            )}

            {cancellableJobCount > 0 && (
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>Job ID</TableCell>
                    <TableCell>Queue</TableCell>
                    <TableCell>Job Set</TableCell>
                    <TableCell>State</TableCell>
                    <TableCell>Submitted Time</TableCell>
                    {showResponseCol && <TableCell>Response</TableCell>}
                  </TableRow>
                </TableHead>

                <TableBody>
                  {cancellableJobsToRenderWithStatus.map(({ job, lastResponseStatus }) => (
                    <TableRow key={job.jobId}>
                      <TableCell>{job.jobId}</TableCell>
                      <TableCell>{job.queue}</TableCell>
                      <TableCell>{job.jobSet}</TableCell>
                      <TableCell>{formatJobState(job.state)}</TableCell>
                      <TableCell>{job.submitted}</TableCell>
                      {showResponseCol && <TableCell>{lastResponseStatus}</TableCell>}
                    </TableRow>
                  ))}
                  {cancellableJobCount > cancellableJobsToRenderWithStatus.length && (
                    <TableRow>
                      <TableCell colSpan={5}>
                        And {cancellableJobCount - cancellableJobsToRenderWithStatus.length} more jobs...
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
            )}
          </>
        )}
      </DialogContent>
    )
  },
)
