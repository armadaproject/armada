import { Checkbox } from "@mui/material"
import { CellContext, Row } from "@tanstack/react-table"
import { ColumnDef, createColumnHelper, VisibilityState } from "@tanstack/table-core"
import { JobStateLabel } from "components/lookoutV2/JobStateLabel"
import { EnumFilterOption } from "components/lookoutV2/JobsTableFilter"
import { isJobGroupRow, JobTableRow } from "models/jobsTableModels"
import { JobState, Match } from "models/lookoutV2Models"

import { formatJobState, formatTimeSince, formatUtcDate } from "./jobsTableFormatters"
import { formatBytes, formatCpu, parseBytes, parseCpu, parseInteger } from "./resourceUtils"
import { JobGroupStateCounts } from "../components/lookoutV2/JobGroupStateCounts"
import { LookoutColumnOrder } from "../containers/lookoutV2/JobsTableContainer"

export type JobTableColumn = ColumnDef<JobTableRow, any>

export enum FilterType {
  Text = "Text",
  Enum = "Enum",
}

export interface JobTableColumnMetadata {
  displayName: string
  isRightAligned?: boolean

  filterType?: FilterType
  enumFilterValues?: EnumFilterOption[]
  defaultMatchType?: Match

  annotation?: {
    annotationKey: string
  }
}

export enum StandardColumnId {
  JobID = "jobId",
  Queue = "queue",
  JobSet = "jobSet",
  State = "state",
  Priority = "priority",
  Owner = "owner",
  CPU = "cpu",
  Memory = "memory",
  EphemeralStorage = "ephemeralStorage",
  GPU = "gpu",
  PriorityClass = "priorityClass",
  TimeSubmittedUtc = "timeSubmittedUtc",
  TimeSubmittedAgo = "timeSubmittedAgo",
  LastTransitionTimeUtc = "lastTransitionTimeUtc",
  TimeInState = "timeInState",
  SelectorCol = "selectorCol",

  Count = "jobCount",
}

export const ANNOTATION_COLUMN_PREFIX = "annotation_"

export type AnnotationColumnId = `annotation_${string}`

export type ColumnId = StandardColumnId | AnnotationColumnId

export const toAnnotationColId = (annotationKey: string): AnnotationColumnId =>
  `${ANNOTATION_COLUMN_PREFIX}${annotationKey}`

export const fromAnnotationColId = (colId: AnnotationColumnId): string => colId.slice(ANNOTATION_COLUMN_PREFIX.length)

export const toColId = (columnId: string | undefined) => columnId as ColumnId

export const isStandardColId = (columnId: string) => (Object.values(StandardColumnId) as string[]).includes(columnId)

export const getColumnMetadata = (column: JobTableColumn) => (column.meta ?? {}) as JobTableColumnMetadata

const columnHelper = createColumnHelper<JobTableRow>()

interface AccessorColumnHelperArgs {
  id: ColumnId
  accessor: Parameters<typeof columnHelper.accessor>[0]
  displayName: string
  additionalOptions?: Partial<Parameters<typeof columnHelper.accessor>[1]>
  additionalMetadata?: Partial<JobTableColumnMetadata>
}

const accessorColumn = ({
  id,
  accessor,
  displayName,
  additionalOptions,
  additionalMetadata,
}: AccessorColumnHelperArgs) => {
  return columnHelper.accessor(accessor, {
    id: id,
    header: displayName,
    enableHiding: true,
    enableSorting: false,
    size: 300,
    minSize: 80,
    ...additionalOptions,
    meta: {
      displayName: displayName,
      ...additionalMetadata,
    } as JobTableColumnMetadata,
  })
}

// Columns will appear in this order by default
export const JOB_COLUMNS: JobTableColumn[] = [
  columnHelper.display({
    id: StandardColumnId.SelectorCol,
    size: 50,
    aggregatedCell: undefined,
    enableColumnFilter: false,
    enableSorting: false,
    enableHiding: false,
    header: ({ table }): JSX.Element => (
      <Checkbox
        checked={table.getIsAllRowsSelected()}
        indeterminate={table.getIsSomeRowsSelected()}
        onChange={(event) => {
          if (!event.currentTarget.checked || table.getIsSomeRowsSelected()) {
            table.toggleAllRowsSelected(false)
            return
          }
          table.toggleAllRowsSelected(true)
        }}
        size="small"
        sx={{ p: 0 }}
      />
    ),
    cell: (item: unknown) => {
      const itemCasted = item as CellContext<JobTableRow, unknown>
      const row = itemCasted.row
      const onClickRowCheckbox = (itemCasted as any).onClickRowCheckbox as (row: Row<JobTableRow>) => void
      return (
        <Checkbox
          checked={row.getIsGrouped() ? row.getIsAllSubRowsSelected() : row.getIsSelected()}
          indeterminate={row.getIsSomeSelected()}
          size="small"
          onClick={(e) => {
            // Do normal flow for when the shift key is pressed
            if (e.shiftKey) {
              return
            }
            if (onClickRowCheckbox !== undefined) {
              onClickRowCheckbox(row)
            }
            e.stopPropagation()
          }}
          sx={{
            p: 0,
            ml: `${row.depth * 6}px`,
          }}
        />
      )
    },
    meta: {
      displayName: "Select Column",
    } as JobTableColumnMetadata,
  }),
  accessorColumn({
    id: StandardColumnId.Queue,
    accessor: "queue",
    displayName: "Queue",
    additionalOptions: {
      enableGrouping: true,
      enableColumnFilter: true,
      size: 300,
    },
    additionalMetadata: {
      filterType: FilterType.Text,
      defaultMatchType: Match.StartsWith,
    },
  }),
  accessorColumn({
    id: StandardColumnId.JobSet,
    accessor: "jobSet",
    displayName: "Job Set",
    additionalOptions: {
      enableGrouping: true,
      enableColumnFilter: true,
      size: 400,
    },
    additionalMetadata: {
      filterType: FilterType.Text,
      defaultMatchType: Match.StartsWith,
    },
  }),
  accessorColumn({
    id: StandardColumnId.JobID,
    accessor: "jobId",
    displayName: "Job ID",
    additionalOptions: {
      enableColumnFilter: true,
      enableSorting: true,
      size: 300,
    },
    additionalMetadata: {
      filterType: FilterType.Text,
      defaultMatchType: Match.Exact, // Job ID does not support startsWith
    },
  }),
  accessorColumn({
    id: StandardColumnId.State,
    accessor: "state",
    displayName: "State",
    additionalOptions: {
      enableGrouping: true,
      enableColumnFilter: true,
      size: 300,
      cell: (cell) => {
        if (
          cell.row.original &&
          isJobGroupRow(cell.row.original) &&
          cell.row.original.stateCounts &&
          cell.row.original.groupedField !== "state"
        ) {
          return <JobGroupStateCounts stateCounts={cell.row.original.stateCounts} />
        } else {
          return (
            <JobStateLabel state={cell.getValue() as JobState}>
              {formatJobState(cell.getValue() as JobState)}
            </JobStateLabel>
          )
        }
      },
    },
    additionalMetadata: {
      filterType: FilterType.Enum,
      enumFilterValues: Object.values(JobState).map((state) => ({
        value: state,
        displayName: formatJobState(state),
      })),
      defaultMatchType: Match.AnyOf,
    },
  }),
  accessorColumn({
    id: StandardColumnId.Count,
    accessor: (jobTableRow) => {
      if (isJobGroupRow(jobTableRow)) {
        return `${jobTableRow.jobCount}`
      }
      return ""
    },
    displayName: "Count",
    additionalOptions: {
      size: 200,
      enableSorting: true,
    },
    additionalMetadata: {
      isRightAligned: true,
    },
  }),
  accessorColumn({
    id: StandardColumnId.Priority,
    accessor: (jobTableRow) => (jobTableRow.priority !== undefined ? `${jobTableRow.priority}` : ""),
    displayName: "Priority",
    additionalOptions: {
      enableColumnFilter: true,
    },
    additionalMetadata: {
      filterType: FilterType.Text,
    },
  }),
  accessorColumn({
    id: StandardColumnId.Owner,
    accessor: "owner",
    displayName: "Owner",
    additionalOptions: {
      enableColumnFilter: true,
    },
    additionalMetadata: {
      filterType: FilterType.Text,
      defaultMatchType: Match.StartsWith,
    },
  }),
  accessorColumn({
    id: StandardColumnId.CPU,
    accessor: (jobTableRow) => (jobTableRow.cpu !== undefined ? formatCpu(jobTableRow.cpu) : ""),
    displayName: "CPUs",
    additionalOptions: {
      enableColumnFilter: true,
    },
    additionalMetadata: {
      filterType: FilterType.Text,
    },
  }),
  accessorColumn({
    id: StandardColumnId.Memory,
    accessor: (jobTableRow) => (jobTableRow.memory !== undefined ? formatBytes(jobTableRow.memory) : ""),
    displayName: "Memory",
    additionalOptions: {
      size: 200,
      enableColumnFilter: true,
    },
    additionalMetadata: {
      filterType: FilterType.Text,
    },
  }),
  accessorColumn({
    id: StandardColumnId.EphemeralStorage,
    accessor: (jobTableRow) =>
      jobTableRow.ephemeralStorage !== undefined ? formatBytes(jobTableRow.ephemeralStorage) : "",
    displayName: "Ephemeral Storage",
    additionalOptions: {
      size: 200,
      enableColumnFilter: true,
    },
    additionalMetadata: {
      filterType: FilterType.Text,
    },
  }),
  accessorColumn({
    id: StandardColumnId.GPU,
    accessor: (jobTableRow) => (jobTableRow.gpu !== undefined ? `${jobTableRow.gpu}` : ""),
    displayName: "GPUs",
    additionalOptions: {
      enableColumnFilter: true,
    },
    additionalMetadata: {
      filterType: FilterType.Text,
    },
  }),
  accessorColumn({
    id: StandardColumnId.PriorityClass,
    accessor: "priorityClass",
    displayName: "Priority Class",
    additionalOptions: {
      enableColumnFilter: true,
    },
    additionalMetadata: {
      filterType: FilterType.Text,
    },
  }),
  accessorColumn({
    id: StandardColumnId.LastTransitionTimeUtc,
    accessor: (jobTableRow) => formatUtcDate(jobTableRow.lastTransitionTime),
    displayName: "Last State Change (UTC)",
    additionalOptions: {
      enableSorting: true,
    },
  }),
  accessorColumn({
    id: StandardColumnId.TimeInState,
    accessor: (jobTableRow) => formatTimeSince(jobTableRow.lastTransitionTime),
    displayName: "Time In State",
    additionalOptions: {
      enableSorting: true,
      size: 200,
    },
  }),
  accessorColumn({
    id: StandardColumnId.TimeSubmittedUtc,
    accessor: (jobTableRow) => formatUtcDate(jobTableRow.submitted),
    displayName: "Time Submitted (UTC)",
    additionalOptions: {
      enableSorting: true,
    },
  }),
  accessorColumn({
    id: StandardColumnId.TimeSubmittedAgo,
    accessor: (jobTableRow) => formatTimeSince(jobTableRow.submitted),
    displayName: "Time Since Submitted",
    additionalOptions: {
      enableSorting: true,
    },
  }),
]

export const DEFAULT_COLUMNS_TO_DISPLAY: Set<ColumnId> = new Set([
  StandardColumnId.SelectorCol,
  StandardColumnId.Queue,
  StandardColumnId.JobSet,
  StandardColumnId.JobID,
  StandardColumnId.State,
  StandardColumnId.TimeInState,
  StandardColumnId.TimeSubmittedUtc,
])

export const DEFAULT_COLUMN_VISIBILITY: VisibilityState = Object.values(StandardColumnId).reduce<VisibilityState>(
  (state, colId) => {
    state[colId] = DEFAULT_COLUMNS_TO_DISPLAY.has(colId)
    return state
  },
  {},
)

export const DEFAULT_COLUMN_ORDER: LookoutColumnOrder = { id: "jobId", direction: "DESC" }

type Formatter = (val: number | string | string[]) => string

interface InputProcessors {
  formatter: Formatter
  parser: (val: string) => number | string | string[]
}

type ParseType = "Cpu" | "Int" | "Bytes"

export const COLUMN_PARSE_TYPES: Record<string, ParseType> = {
  [StandardColumnId.CPU]: "Cpu",
  [StandardColumnId.Memory]: "Bytes",
  [StandardColumnId.EphemeralStorage]: "Bytes",
  [StandardColumnId.GPU]: "Int",
  [StandardColumnId.Priority]: "Int",
}

export const INPUT_PROCESSORS: Record<ParseType, InputProcessors> = {
  ["Cpu"]: {
    formatter: formatCpu as Formatter,
    parser: parseCpu,
  },
  ["Int"]: {
    formatter: (val) => `${val}`,
    parser: parseInteger,
  },
  ["Bytes"]: {
    formatter: formatBytes as Formatter,
    parser: parseBytes,
  },
}

export const DEFAULT_COLUMN_MATCHES: Record<string, Match> = {
  [StandardColumnId.Queue]: Match.StartsWith,
  [StandardColumnId.JobSet]: Match.StartsWith,
  [StandardColumnId.JobID]: Match.Exact,
  [StandardColumnId.State]: Match.AnyOf,
  [StandardColumnId.Owner]: Match.StartsWith,
  [StandardColumnId.CPU]: Match.Exact,
  [StandardColumnId.Memory]: Match.Exact,
  [StandardColumnId.EphemeralStorage]: Match.Exact,
  [StandardColumnId.GPU]: Match.Exact,
  [StandardColumnId.Priority]: Match.Exact,
  [StandardColumnId.PriorityClass]: Match.Exact,
}

export const VALID_COLUMN_MATCHES: Record<string, Match[]> = {
  [StandardColumnId.JobID]: [Match.Exact],
  [StandardColumnId.Queue]: [Match.Exact, Match.StartsWith, Match.Contains],
  [StandardColumnId.JobSet]: [Match.Exact, Match.StartsWith, Match.Contains],
  [StandardColumnId.Owner]: [Match.Exact, Match.StartsWith, Match.Contains],
  [StandardColumnId.State]: [Match.AnyOf],
  [StandardColumnId.CPU]: [
    Match.Exact,
    Match.GreaterThan,
    Match.LessThan,
    Match.GreaterThanOrEqual,
    Match.LessThanOrEqual,
  ],
  [StandardColumnId.Memory]: [
    Match.Exact,
    Match.GreaterThan,
    Match.LessThan,
    Match.GreaterThanOrEqual,
    Match.LessThanOrEqual,
  ],
  [StandardColumnId.EphemeralStorage]: [
    Match.Exact,
    Match.GreaterThan,
    Match.LessThan,
    Match.GreaterThanOrEqual,
    Match.LessThanOrEqual,
  ],
  [StandardColumnId.GPU]: [
    Match.Exact,
    Match.GreaterThan,
    Match.LessThan,
    Match.GreaterThanOrEqual,
    Match.LessThanOrEqual,
  ],
  [StandardColumnId.Priority]: [
    Match.Exact,
    Match.GreaterThan,
    Match.LessThan,
    Match.GreaterThanOrEqual,
    Match.LessThanOrEqual,
  ],
  [StandardColumnId.PriorityClass]: [Match.Exact, Match.StartsWith, Match.Contains],
  [ANNOTATION_COLUMN_PREFIX]: [Match.Exact, Match.StartsWith, Match.Contains],
}

export const createAnnotationColumn = (annotationKey: string): JobTableColumn => {
  return accessorColumn({
    id: toAnnotationColId(annotationKey),
    accessor: (jobTableRow) => jobTableRow.annotations?.[annotationKey],
    displayName: annotationKey,
    additionalOptions: {
      enableColumnFilter: true,
      enableGrouping: true,
    },
    additionalMetadata: {
      annotation: {
        annotationKey,
      },
      filterType: FilterType.Text,
      defaultMatchType: Match.StartsWith,
    },
  })
}

export const getAnnotationKeyCols = (cols: JobTableColumn[]): string[] => {
  return cols
    .filter((col) => col.id?.startsWith(ANNOTATION_COLUMN_PREFIX))
    .map((col) => fromAnnotationColId(col.id as AnnotationColumnId))
}
