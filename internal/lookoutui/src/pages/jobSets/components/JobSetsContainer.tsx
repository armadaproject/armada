import { useCallback, useEffect, useMemo, useState } from "react"

import { ErrorBoundary } from "react-error-boundary"
import { useLocation, useNavigate, useParams } from "react-router-dom"

import { StandardColumnId } from "../../../common/jobsTableColumns"
import { ApiResult, RequestStatus, selectItem } from "../../../common/utils"
import { AlertErrorFallback } from "../../../components/AlertErrorFallback"
import { useCustomSnackbar } from "../../../components/hooks/useCustomSnackbar"
import { JobSet, JobSetsOrderByColumn } from "../../../models/lookoutModels"
import { JOBS } from "../../../pathnames"
import JobSetsLocalStorageService, { JobSetsPrefs } from "../../../services/JobSetsLocalStorageService"
import JobSetsQueryParamsService from "../../../services/JobSetsQueryParamsService"
import {
  DEFAULT_PREFERENCES,
  JobsTablePreferences,
  stringifyQueryParams,
  toQueryStringSafe,
} from "../../../services/lookout/JobsTablePreferencesService"
import { useGetJobSets } from "../../../services/lookout/useGetJobSets"

import CancelJobSetsDialog, { getCancellableJobSets } from "./CancelJobSetsDialog"
import JobSets from "./JobSets"
import ReprioritizeJobSetsDialog, { getReprioritizableJobSets } from "./ReprioritizeJobSetsDialog"

export interface JobSetsContainerProps {
  jobSetsAutoRefreshMs: number | undefined
}

const DEFAULT_PREFS: JobSetsPrefs = {
  queue: "",
  autoRefresh: true,
  orderByColumn: "submitted",
  orderByDesc: true,
  activeOnly: false,
}

export default function JobSetsContainer({ jobSetsAutoRefreshMs }: JobSetsContainerProps) {
  const location = useLocation()
  const navigate = useNavigate()
  const params = useParams()
  const openSnackbar = useCustomSnackbar()

  const localStorageService = useMemo(() => new JobSetsLocalStorageService(), [])
  const queryParamsService = useMemo(() => new JobSetsQueryParamsService({ location, navigate, params }), [])

  const [prefs] = useState<JobSetsPrefs>(() => {
    const initial = { ...DEFAULT_PREFS }
    localStorageService.updateState(initial)
    queryParamsService.updateState(initial)
    return initial
  })

  const [queue, setQueueState] = useState(prefs.queue)
  const [orderByColumn, setOrderByColumn] = useState(prefs.orderByColumn)
  const [orderByDesc, setOrderByDesc] = useState(prefs.orderByDesc)
  const [activeOnly, setActiveOnly] = useState(prefs.activeOnly)
  const [autoRefresh, setAutoRefresh] = useState(prefs.autoRefresh)

  const [selectedJobSets, setSelectedJobSets] = useState<Map<string, JobSet>>(new Map())
  const [lastSelectedIndex, setLastSelectedIndex] = useState(0)
  const [cancelJobSetsIsOpen, setCancelJobSetsIsOpen] = useState(false)
  const [reprioritizeJobSetsIsOpen, setReprioritizeJobSetsIsOpen] = useState(false)

  const { data, error, errorUpdatedAt, isFetching, refetch } = useGetJobSets({
    queue,
    activeOnly,
    orderByColumn,
    orderByDesc,
    autoRefresh,
    autoRefreshMs: jobSetsAutoRefreshMs,
  })
  const jobSets = data ?? []

  useEffect(() => {
    const currentPrefs: JobSetsPrefs = { queue, autoRefresh, orderByColumn, orderByDesc, activeOnly }
    localStorageService.saveState(currentPrefs)
    queryParamsService.saveState(currentPrefs)
  }, [queue, autoRefresh, orderByColumn, orderByDesc, activeOnly, localStorageService, queryParamsService])

  useEffect(() => {
    if (error !== null) {
      openSnackbar(`Failed to load job sets for queue ${queue}: ${error}`, "error")
    }
    // Keyed on errorUpdatedAt (not error) so repeated identical failures, such as
    // retrying refresh while offline, each surface a fresh snackbar.
  }, [errorUpdatedAt])

  const deselectAll = useCallback(() => {
    setSelectedJobSets(new Map())
    setLastSelectedIndex(0)
  }, [])

  const selectAll = useCallback(() => {
    const selected = new Map<string, JobSet>()
    jobSets.forEach((jobSet) => selected.set(jobSet.jobSetId, jobSet))
    setSelectedJobSets(selected)
    setLastSelectedIndex(0)
  }, [jobSets])

  const selectJobSet = useCallback(
    (index: number, selected: boolean) => {
      if (index < 0 || index >= jobSets.length) {
        return
      }
      const jobSet = jobSets[index]
      const newSelected = new Map(selectedJobSets)
      selectItem(jobSet.jobSetId, jobSet, newSelected, selected)
      setSelectedJobSets(newSelected)
      setLastSelectedIndex(index)
    },
    [jobSets, selectedJobSets],
  )

  const shiftSelectJobSet = useCallback(
    (index: number, selected: boolean) => {
      if (index < 0 || index >= jobSets.length) {
        return
      }
      const [start, end] = [lastSelectedIndex, index].sort((a, b) => a - b)
      const newSelected = new Map(selectedJobSets)
      for (let i = start; i <= end; i++) {
        const jobSet = jobSets[i]
        selectItem(jobSet.jobSetId, jobSet, newSelected, selected)
      }
      setSelectedJobSets(newSelected)
      setLastSelectedIndex(index)
    },
    [jobSets, lastSelectedIndex, selectedJobSets],
  )

  const handleApiResult = useCallback(
    (result: ApiResult) => {
      if (result === "Success") {
        deselectAll()
        refetch()
      } else if (result === "Partial success") {
        refetch()
      }
    },
    [deselectAll, refetch],
  )

  const onJobSetStateClick = useCallback(
    (rowIndex: number, state: string) => {
      const jobSet = jobSets[rowIndex]
      const prefs: JobsTablePreferences = {
        ...DEFAULT_PREFERENCES,
        filters: [
          { id: StandardColumnId.Queue, value: jobSet.queue },
          { id: StandardColumnId.State, value: [state] },
          { id: StandardColumnId.JobSet, value: jobSet.jobSetId },
        ],
      }
      navigate({ pathname: JOBS, search: stringifyQueryParams(toQueryStringSafe(prefs)) })
    },
    [jobSets, navigate],
  )

  const selectedJobSetsArray = Array.from(selectedJobSets.values())
  const getJobSetsRequestStatus: RequestStatus = isFetching ? "Loading" : "Idle"

  return (
    <>
      <CancelJobSetsDialog
        isOpen={cancelJobSetsIsOpen}
        queue={queue}
        selectedJobSets={selectedJobSetsArray}
        onResult={handleApiResult}
        onClose={() => setCancelJobSetsIsOpen(false)}
      />
      <ReprioritizeJobSetsDialog
        isOpen={reprioritizeJobSetsIsOpen}
        queue={queue}
        selectedJobSets={selectedJobSetsArray}
        onResult={handleApiResult}
        onClose={() => setReprioritizeJobSetsIsOpen(false)}
      />
      <ErrorBoundary FallbackComponent={AlertErrorFallback}>
        <JobSets
          canCancel={getCancellableJobSets(selectedJobSetsArray).length > 0}
          canReprioritize={getReprioritizableJobSets(selectedJobSetsArray).length > 0}
          queue={queue}
          jobSets={jobSets}
          selectedJobSets={selectedJobSets}
          getJobSetsRequestStatus={getJobSetsRequestStatus}
          autoRefresh={autoRefresh}
          orderByColumn={orderByColumn}
          orderByDesc={orderByDesc}
          activeOnly={activeOnly}
          onQueueChange={setQueueState}
          onOrderChange={(column: JobSetsOrderByColumn, desc: boolean) => {
            setOrderByColumn(column)
            setOrderByDesc(desc)
          }}
          onActiveOnlyChange={setActiveOnly}
          onRefresh={() => refetch()}
          onSelectJobSet={selectJobSet}
          onShiftSelectJobSet={shiftSelectJobSet}
          onDeselectAllClick={deselectAll}
          onSelectAllClick={selectAll}
          onCancelJobSetsClick={() => setCancelJobSetsIsOpen(true)}
          onToggleAutoRefresh={jobSetsAutoRefreshMs !== undefined ? setAutoRefresh : undefined}
          onReprioritizeJobSetsClick={() => setReprioritizeJobSetsIsOpen(true)}
          onJobSetStateClick={onJobSetStateClick}
        />
      </ErrorBoundary>
    </>
  )
}
