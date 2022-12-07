import { Button, CircularProgress, Dialog, DialogActions, DialogContent, DialogContentText, DialogTitle, LinearProgress, TableRow, TableBody, TableCell, TableHead, Table, Typography, Alert } from "@mui/material"
import { Refresh, Dangerous } from '@mui/icons-material';
import { LoadingButton } from "@mui/lab"
import _ from "lodash"
import { isTerminatedJobState, Job, JobFilter, JobId, JobStates } from "models/lookoutV2Models"
import { useSnackbar } from "notistack"
import { memo, useCallback, useEffect, useMemo, useState } from "react"
import GetJobsService from "services/lookoutV2/GetJobsService"
import { simulateApiWait } from "utils/fakeJobsUtils"
import styles from "./CancelDialog.module.css"
import { pl } from "utils";

const MAX_JOBS_PER_REQUEST = 1000

const getAllJobsMatchingFilters = async (filters: JobFilter[], getJobsService: GetJobsService): Promise<Job[]> => {
  const receivedJobs: Job[] = []
  let continuePaginating = true
  while (continuePaginating) {
    const { jobs, totalJobs } = await getJobsService.getJobs(
      filters,
      { direction: "DESC", field: "jobId" },
      receivedJobs.length,
      MAX_JOBS_PER_REQUEST,
      undefined
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
  getJobsService: GetJobsService
}
export const CancelDialog = ({ onClose, selectedItemFilters, getJobsService }: CancelDialogProps) => {
  const [isLoading, setIsLoading] = useState(true)
  const [isCancelling, setIsCancelling] = useState(false)
  const [hasCancelled, setHasCancelled] = useState(false)
  const [selectedJobs, setSelectedJobs] = useState<Job[]>([])
  const { enqueueSnackbar } = useSnackbar();

  const cancellableJobs = useMemo(() => selectedJobs.filter(job => !isTerminatedJobState(job.state)), [selectedJobs])
  useEffect(() => {
    if (cancellableJobs.length > 0) {
      setHasCancelled(false)
    }
  }, [cancellableJobs])

  const fetchJobs = useCallback(async () => {
    const jobsBySelectedItem = await Promise.all(
      selectedItemFilters.map(async (filters) => await getAllJobsMatchingFilters(filters, getJobsService))
    )
    const uniqueJobsToCancel = _.uniqBy(jobsBySelectedItem.flat(), job => job.jobId)
    const sortedJobs = _.orderBy(uniqueJobsToCancel, job => job.jobId, 'desc')

    setSelectedJobs(sortedJobs)
    setIsLoading(false)
  }, [selectedItemFilters, getJobsService])

  useEffect(() => {
    fetchJobs().catch(console.error)
  }, [fetchJobs])

  useEffect(() => {
    async function cancelJobs() {
      if (!isCancelling) {
        return
      }

      await simulateApiWait();

      enqueueSnackbar("Successfully began cancellation. Jobs may take some time to cancel, but you may navigate away.", { variant: "success" })
      setIsCancelling(false)
      setHasCancelled(true)
    }

    cancelJobs().catch(console.error)
  }, [isCancelling])

  const handleCancelJobs = useCallback(async () => {
    setIsCancelling(true)
  }, [])

  const handleRefetch = () => {
    setIsLoading(true)
    fetchJobs().catch(console.error)
  }

  const jobsToRender = useMemo(() => cancellableJobs.slice(0, 1000), [cancellableJobs])
  return (
    <Dialog
      open={true}
      onClose={onClose}
      fullWidth
      maxWidth="xl"
    >
      <DialogTitle>
        Cancel {isLoading ? "jobs" : pl(cancellableJobs, 'job')}
      </DialogTitle>
      <CancelDialogBody
        isLoading={isLoading}
        cancellableJobsToRender={jobsToRender}
        cancellableJobCount={cancellableJobs.length}
        selectedJobCount={selectedJobs.length}
      />
      <DialogActions>
        <Button onClick={onClose}>Close</Button>
        <Button onClick={handleRefetch} disabled={isLoading || isCancelling} variant="outlined" endIcon={<Refresh />}>Refetch jobs</Button>
        <LoadingButton onClick={handleCancelJobs} loading={isCancelling} disabled={isLoading || hasCancelled || cancellableJobs.length === 0} variant="contained" endIcon={<Dangerous />}>
          Cancel {isLoading ? "jobs" : pl(cancellableJobs, 'job')}
        </LoadingButton>
      </DialogActions>
    </Dialog>
  )
}

interface CancelDialogBodyProps {
  isLoading: boolean
  cancellableJobsToRender: Job[]
  cancellableJobCount: number
  selectedJobCount: number
}
const CancelDialogBody = memo(({ isLoading, cancellableJobsToRender, cancellableJobCount, selectedJobCount }: CancelDialogBodyProps) => {
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
            <Alert severity="info" sx={{marginBottom: "0.5em"}}>
              {pl(selectedJobCount, 'job is', 'jobs are')} selected, but only {pl(cancellableJobCount, 'job is', 'jobs are')} in a cancellable (non-terminated) state.
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
                <TableCell>Owner</TableCell>
              </TableRow>
            </TableHead>

            <TableBody>
              {cancellableJobsToRender.map(job => (
                <TableRow key={job.jobId}>
                  <TableCell>{job.jobId}</TableCell>
                  <TableCell>{job.queue}</TableCell>
                  <TableCell>{job.jobSet}</TableCell>
                  <TableCell>{job.state}</TableCell>
                  <TableCell>{job.owner}</TableCell>
                </TableRow>
              ))}
              {cancellableJobCount > cancellableJobsToRender.length && (
                <TableRow>
                  <TableCell colSpan={5}>
                    And {cancellableJobCount - cancellableJobsToRender.length} more jobs...
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
          )}
        </>
      )}

    </DialogContent>)
})