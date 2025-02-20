import { forwardRef, useCallback, useEffect, useState } from "react"

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
import { TableVirtuoso, TableComponents } from "react-virtuoso"

import { JobState, jobStateColors, jobStateIcons } from "../../models/lookoutModels"
import { JobSet } from "../../services/JobService"
import { formatJobState } from "../../utils/jobsTableFormatters"
import { JobStateCountChip } from "../lookout/JobStateCountChip"

const JOB_STATES_TO_DISPLAY = [
  [JobState.Queued, "jobsQueued"],
  [JobState.Pending, "jobsPending"],
  [JobState.Running, "jobsRunning"],
  [JobState.Succeeded, "jobsSucceeded"],
  [JobState.Failed, "jobsFailed"],
  [JobState.Cancelled, "jobsCancelled"],
] as const

interface TableContext {
  selectedJobSetIds: Set<string>
}

const NoWrapTableCell = styled(TableCell)({ textWrap: "nowrap" })
const MinWidthTableCell = styled(NoWrapTableCell)({ width: "0%" })
const MaxWidthTableCell = styled(TableCell)({ width: "100%" })

const StyledTable = styled(Table)({
  borderCollapse: "separate",
})

const TableHeaderRowCell = styled(TableCell)(({ theme }) => ({ background: theme.palette.background.paper }))

const TableHeaderRowMinWidthCell = styled(MinWidthTableCell)(({ theme }) => ({
  background: theme.palette.background.paper,
}))

const VirtuosoTableComponents: TableComponents<JobSet, TableContext> = {
  Scroller: forwardRef<HTMLDivElement>((props, ref) => <TableContainer {...props} ref={ref} />),
  Table: (props) => <StyledTable {...props} size="small" />,
  TableHead: forwardRef<HTMLTableSectionElement>((props, ref) => <TableHead {...props} ref={ref} />),
  TableRow: ({ context, ...props }) => (
    <TableRow {...props} hover selected={context?.selectedJobSetIds.has(props.item.jobSetId)} />
  ),
  TableBody: forwardRef<HTMLTableSectionElement>((props, ref) => <TableBody {...props} ref={ref} />),
}

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
    <TableVirtuoso<JobSet, TableContext>
      data={jobSets}
      components={VirtuosoTableComponents}
      context={{ selectedJobSetIds: new Set(selectedJobSets.keys()) }}
      fixedHeaderContent={() => (
        <TableRow>
          <TableHeaderRowCell padding="checkbox">
            <Checkbox
              color="primary"
              inputProps={{
                "aria-label": "select all job sets",
              }}
              indeterminate={0 < selectedJobSets.size && selectedJobSets.size < jobSets.length}
              checked={selectedJobSets.size === jobSets.length}
              onChange={(_, checked) => (checked ? onSelectAllClick() : onDeselectAllClick())}
            />
          </TableHeaderRowCell>
          <TableHeaderRowCell>Job Set</TableHeaderRowCell>
          <TableHeaderRowCell>
            <TableSortLabel active direction={newestFirst ? "desc" : "asc"} onClick={() => onOrderChange(!newestFirst)}>
              Submission time
              <Box component="span" sx={visuallyHidden}>
                {newestFirst ? "sorted descending" : "sorted ascending"}
              </Box>
            </TableSortLabel>
          </TableHeaderRowCell>
          {JOB_STATES_TO_DISPLAY.map(([jobState]) => {
            const Icon = jobStateIcons[jobState]
            return (
              <TableHeaderRowMinWidthCell key={jobState} align="right">
                <Stack direction="row" spacing={1} alignItems="center" display="inline">
                  <span>{formatJobState(jobState)}</span>
                  <Icon fontSize="inherit" color={jobStateColors[jobState]} />
                </Stack>
              </TableHeaderRowMinWidthCell>
            )
          })}
        </TableRow>
      )}
      itemContent={(jobSetIndex, { jobSetId, latestSubmissionTime, ...jobSetRest }) => {
        const rowSelected = selectedJobSets.has(jobSetId)
        return (
          <>
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
            <MaxWidthTableCell>{jobSetId}</MaxWidthTableCell>
            <NoWrapTableCell>{latestSubmissionTime}</NoWrapTableCell>
            {JOB_STATES_TO_DISPLAY.map(([jobState, jobSetKey]) => (
              <MinWidthTableCell key={jobState} align="right">
                <div>
                  <JobStateCountChip
                    state={jobState}
                    count={jobSetRest[jobSetKey]}
                    onClick={() => onJobSetStateClick(jobSetIndex, jobState)}
                  />
                </div>
              </MinWidthTableCell>
            ))}
          </>
        )
      }}
    />
  )
}
