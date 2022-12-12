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
  TextField,
  TableContainer,
} from "@mui/material"
import _ from "lodash"
import { isTerminatedJobState, Job, JobFilter, JobId } from "models/lookoutV2Models"
import { useSnackbar } from "notistack"
import { IGetJobsService } from "services/lookoutV2/GetJobsService"
import { UpdateJobsService } from "services/lookoutV2/UpdateJobsService"
import { pl } from "utils"

import styles from "./ReprioritiseDialog.module.css"

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
  const [isLoadingJobs, setIsLoadingJobs] = useState(true)
  const [newPriority, setNewPriority] = useState<number | undefined>(undefined)
  const [isReprioritising, setIsReprioritising] = useState(false)
  const [hasAttemptedReprioritise, setHasAttemptedReprioritise] = useState(false)
  const [selectedJobs, setSelectedJobs] = useState<Job[]>([])
  const [jobIdsToReprioritiseResponses, setJobIdsToReprioritiseResponses] = useState<Record<JobId, string>>({})
  const { enqueueSnackbar } = useSnackbar()

  const reprioritiselableJobs = useMemo(
    () => selectedJobs.filter((job) => !isTerminatedJobState(job.state)),
    [selectedJobs],
  )
  useEffect(() => {
    if (reprioritiselableJobs.length > 0) {
      setHasAttemptedReprioritise(false)
    }
  }, [reprioritiselableJobs])

  const fetchSelectedJobs = useCallback(async () => {
    const jobsBySelectedItem = await Promise.all(
      selectedItemFilters.map(async (filters) => await getAllJobsMatchingFilters(filters, getJobsService)),
    )
    const uniqueJobsToreprioritise = _.uniqBy(jobsBySelectedItem.flat(), (job) => job.jobId)
    const sortedJobs = _.orderBy(uniqueJobsToreprioritise, (job) => job.jobId, "desc")

    setSelectedJobs(sortedJobs)
    setIsLoadingJobs(false)
  }, [selectedItemFilters, getJobsService])

  useEffect(() => {
    fetchSelectedJobs().catch(console.error)
  }, [fetchSelectedJobs])

  useEffect(() => {
    async function reprioritiseJobs() {
      if (!isReprioritising || newPriority === undefined) {
        return
      }

      const jobIdsToreprioritise = reprioritiselableJobs.map((job) => job.jobId)
      const response = await updateJobsService.reprioritiseJobs(jobIdsToreprioritise, newPriority)

      if (response.failedJobIds.length === 0) {
        enqueueSnackbar(
          "Successfully began reprioritising. Jobs may take some time to reprioritise, but you may navigate away.",
          { variant: "success" },
        )
      } else if (response.successfulJobIds.length === 0) {
        enqueueSnackbar("All jobs failed to reprioritise. See table for error responses.", { variant: "error" })
      } else {
        enqueueSnackbar("Some jobs failed to reprioritise. See table for error responses.", { variant: "warning" })
      }

      const newResponseStatus = { ...jobIdsToReprioritiseResponses }
      response.successfulJobIds.map((jobId) => (newResponseStatus[jobId] = "Success"))
      response.failedJobIds.map(({ jobId, errorReason }) => (newResponseStatus[jobId] = errorReason))

      setJobIdsToReprioritiseResponses(newResponseStatus)
      setIsReprioritising(false)
      setHasAttemptedReprioritise(true)
    }

    reprioritiseJobs().catch(console.error)
  }, [isReprioritising, newPriority])

  const handlereprioritiseJobs = useCallback(async () => {
    setIsReprioritising(true)
  }, [])

  const handleRefetch = () => {
    setIsLoadingJobs(true)
    setJobIdsToReprioritiseResponses({})
    fetchSelectedJobs().catch(console.error)
  }

  const jobsToRender = useMemo(() => {
    return reprioritiselableJobs.slice(0, 1000).map((job) => ({
      job,
      lastResponseStatus: jobIdsToReprioritiseResponses[job.jobId],
    }))
  }, [reprioritiselableJobs, jobIdsToReprioritiseResponses])
  return (
    <Dialog open={true} onClose={onClose} fullWidth maxWidth="xl">
      <DialogTitle>Reprioritise {isLoadingJobs ? "jobs" : pl(reprioritiselableJobs, "job")}</DialogTitle>
      <ReprioritiseDialogBody
        isLoading={isLoadingJobs}
        reprioritiselableJobsToRenderWithStatus={jobsToRender}
        reprioritiselableJobCount={reprioritiselableJobs.length}
        selectedJobCount={selectedJobs.length}
        newPriority={newPriority}
        onNewPriority={setNewPriority}
      />
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
          onClick={handlereprioritiseJobs}
          loading={isReprioritising}
          disabled={
            isLoadingJobs || hasAttemptedReprioritise || reprioritiselableJobs.length === 0 || newPriority === undefined
          }
          variant="contained"
          endIcon={<Dangerous />}
        >
          Reprioritise {isLoadingJobs ? "jobs" : pl(reprioritiselableJobs, "job")}
        </LoadingButton>
      </DialogActions>
    </Dialog>
  )
}

interface ReprioritiseDialogBodyProps {
  isLoading: boolean
  reprioritiselableJobsToRenderWithStatus: { job: Job; lastResponseStatus: string | undefined }[]
  reprioritiselableJobCount: number
  selectedJobCount: number
  newPriority: number | undefined
  onNewPriority: (newPriority?: number) => void
}
const ReprioritiseDialogBody = memo(
  ({
    isLoading,
    reprioritiselableJobsToRenderWithStatus,
    reprioritiselableJobCount: reprioritisableJobs,
    selectedJobCount,
    newPriority,
    onNewPriority,
  }: ReprioritiseDialogBodyProps) => {
    const showResponseCol = reprioritiselableJobsToRenderWithStatus.some(
      ({ lastResponseStatus }) => lastResponseStatus !== undefined,
    )
    return (
      <DialogContent sx={{ display: "flex", flexDirection: "column" }}>
        {isLoading && (
          <div className={styles.loadingInfo}>
            Fetching info on selected jobs...
            <CircularProgress variant="indeterminate" />
          </div>
        )}

        {!isLoading && (
          <>
            {reprioritisableJobs > 0 && reprioritisableJobs < selectedJobCount && (
              <Alert severity="info" sx={{ marginBottom: "0.5em" }}>
                {pl(selectedJobCount, "job is", "jobs are")} selected, but only{" "}
                {pl(reprioritisableJobs, "job is", "jobs are")} in a reprioritisable (non-terminated) state.
              </Alert>
            )}

            {reprioritisableJobs === 0 && (
              <Alert severity="success">
                All selected jobs are in a terminated state already, therefore there is nothing to reprioritise.
              </Alert>
            )}

            {reprioritisableJobs > 0 && (
              <TableContainer>
                <Table size="small" stickyHeader>
                  <TableHead>
                    <TableRow>
                      <TableCell>Job ID</TableCell>
                      <TableCell>Queue</TableCell>
                      <TableCell>Job Set</TableCell>
                      <TableCell>Priority</TableCell>
                      <TableCell>Submitted Time</TableCell>
                      {showResponseCol && <TableCell>Response</TableCell>}
                    </TableRow>
                  </TableHead>

                  <TableBody>
                    {reprioritiselableJobsToRenderWithStatus.map(({ job, lastResponseStatus }) => (
                      <TableRow key={job.jobId}>
                        <TableCell>{job.jobId}</TableCell>
                        <TableCell>{job.queue}</TableCell>
                        <TableCell>{job.jobSet}</TableCell>
                        <TableCell>{job.priority}</TableCell>
                        <TableCell>{job.submitted}</TableCell>
                        {showResponseCol && <TableCell>{lastResponseStatus}</TableCell>}
                      </TableRow>
                    ))}
                    {reprioritisableJobs > reprioritiselableJobsToRenderWithStatus.length && (
                      <TableRow>
                        <TableCell colSpan={5}>
                          And {reprioritisableJobs - reprioritiselableJobsToRenderWithStatus.length} more jobs...
                        </TableCell>
                      </TableRow>
                    )}
                  </TableBody>
                </Table>
              </TableContainer>
            )}

            <TextField
              // className={styles.lookoutDialogFixed}
              value={newPriority ?? ""}
              autoFocus={true}
              label={"New priority for jobs"}
              helperText="(0 = highest priority)"
              margin={"normal"}
              type={"text"}
              required
              inputProps={{ inputMode: "numeric", pattern: "[0-9]+" }}
              // error={!props.isValid}
              // helperText={!props.isValid ? "Value must be a number >= 0" : " "}
              onChange={(event) => {
                const val = event.target.value
                const num = Number(event.target.value)
                if (val.length > 0 && !Number.isNaN(num)) {
                  onNewPriority(num)
                } else {
                  onNewPriority(undefined)
                }
              }}
              sx={{ maxWidth: "250px" }}
            />
          </>
        )}
      </DialogContent>
    )
  },
)
