import { useCallback, useEffect, useState } from "react"

import {
  Alert,
  Box,
  Checkbox,
  Stack,
  styled,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TableSortLabel,
} from "@mui/material"
import { visuallyHidden } from "@mui/utils"
import { Truncate } from "@re-dev/react-truncate"

import { JobState, jobStateColors, jobStateIcons } from "../../models/lookoutV2Models"
import { JobSet } from "../../services/JobService"
import { formatJobState } from "../../utils/jobsTableFormatters"
import { JobStateCountChip } from "../lookoutV2/JobStateCountChip"

const JOB_STATES_TO_DISPLAY = [
  [JobState.Queued, "jobsQueued"],
  [JobState.Pending, "jobsPending"],
  [JobState.Running, "jobsRunning"],
  [JobState.Succeeded, "jobsSucceeded"],
  [JobState.Failed, "jobsFailed"],
  [JobState.Cancelled, "jobsCancelled"],
] as const

const MinWidthTableCell = styled(TableCell)({ width: "0%", textWrap: "nowrap" })

interface JobSetTableProps {
  queue: string
  jobSets: JobSet[]
  selectedJobSets: Map<string, JobSet>
  newestFirst: boolean
  onSelectJobSet: (index: number, selected: boolean) => void
  onShiftSelectJobSet: (index: number, selected: boolean) => void
  onDeselectAllClick: () => void
  onSelectAllClick: () => void
  onOrderChange: (newestFirst: boolean) => void
  onJobSetStateClick(rowIndex: number, state: string): void
}

export default function JobSetTable({
  queue,
  jobSets,
  selectedJobSets,
  newestFirst,
  onSelectJobSet,
  onShiftSelectJobSet,
  onDeselectAllClick,
  onSelectAllClick,
  onOrderChange,
  onJobSetStateClick,
}: JobSetTableProps) {
  const [shiftKeyPressed, setShiftKeyPressed] = useState(false)
  const onKeyDown = useCallback((ev: KeyboardEvent) => {
    if (ev.key === "Shift") {
      setShiftKeyPressed(true)
    }
  }, [])
  const onKeyUp = useCallback((ev: KeyboardEvent) => {
    if (ev.key === "Shift") {
      setShiftKeyPressed(false)
    }
  }, [])

  useEffect(() => {
    document.addEventListener("keydown", onKeyDown, false)
    document.addEventListener("keyup", onKeyUp, false)

    return () => {
      document.removeEventListener("keydown", onKeyDown, false)
      document.removeEventListener("keyup", onKeyUp, false)
    }
  }, [onKeyDown, onKeyUp])

  if (queue === "") {
    return <Alert severity="info">Enter a queue name into the "Queue" field to view job sets.</Alert>
  }

  if (jobSets.length === 0) {
    return <Alert severity="warning">No job sets found for this queue.</Alert>
  }

  return (
    <TableContainer>
      <Table size="small">
        <TableHead>
          <TableRow>
            <TableCell padding="checkbox">
              <Checkbox
                color="primary"
                inputProps={{
                  "aria-label": "select all job sets",
                }}
                indeterminate={0 < selectedJobSets.size && selectedJobSets.size < jobSets.length}
                checked={selectedJobSets.size === jobSets.length}
                onChange={(_, checked) => (checked ? onSelectAllClick() : onDeselectAllClick())}
              />
            </TableCell>
            <TableCell>Job Set</TableCell>
            <TableCell>
              <TableSortLabel
                active
                direction={newestFirst ? "desc" : "asc"}
                onClick={() => onOrderChange(!newestFirst)}
              >
                Submission time
                <Box component="span" sx={visuallyHidden}>
                  {newestFirst ? "sorted descending" : "sorted ascending"}
                </Box>
              </TableSortLabel>
            </TableCell>
            {JOB_STATES_TO_DISPLAY.map(([jobState]) => {
              const Icon = jobStateIcons[jobState]
              return (
                <MinWidthTableCell key={jobState} align="right">
                  <Stack direction="row" spacing={1} alignItems="center" display="inline">
                    <span>{formatJobState(jobState)}</span>
                    <Icon fontSize="inherit" color={jobStateColors[jobState]} />
                  </Stack>
                </MinWidthTableCell>
              )
            })}
          </TableRow>
        </TableHead>
        <TableBody>
          {jobSets.map(({ jobSetId, latestSubmissionTime, ...jobSetRest }, jobSetIndex) => {
            const rowSelected = selectedJobSets.has(jobSetId)
            return (
              <TableRow key={jobSetId} hover selected={rowSelected}>
                <TableCell padding="checkbox">
                  <Checkbox
                    color="primary"
                    inputProps={{
                      "aria-label": `select job set ${jobSetId}`,
                    }}
                    checked={rowSelected}
                    onChange={(_, checked) =>
                      shiftKeyPressed ? onShiftSelectJobSet(jobSetIndex, checked) : onSelectJobSet(jobSetIndex, checked)
                    }
                  />
                </TableCell>
                <TableCell>
                  <Truncate lines={1}>{jobSetId}</Truncate>
                </TableCell>
                <TableCell>{latestSubmissionTime}</TableCell>
                {JOB_STATES_TO_DISPLAY.map(([jobState, jobSetKey]) => (
                  <MinWidthTableCell key={jobState} align="right">
                    <JobStateCountChip
                      state={jobState}
                      count={jobSetRest[jobSetKey]}
                      onClick={() => onJobSetStateClick(jobSetIndex, jobState)}
                    />
                  </MinWidthTableCell>
                ))}
              </TableRow>
            )
          })}
        </TableBody>
      </Table>
    </TableContainer>
  )
}
