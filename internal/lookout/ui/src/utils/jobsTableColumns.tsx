import { Checkbox } from "@mui/material"
import { ColumnDef, createColumnHelper, VisibilityState } from "@tanstack/table-core"
import { JobStateLabel } from "components/lookoutV2/JobStateLabel"
import { EnumFilterOption } from "components/lookoutV2/JobsTableFilter"
import { isJobGroupRow, JobTableRow } from "models/jobsTableModels"
import { JobState, Match } from "models/lookoutV2Models"

import { LookoutColumnOrder } from "../containers/lookoutV2/JobsTableContainer"
import { formatBytes, formatCPU, formatJobState, formatTimeSince, formatUtcDate } from "./jobsTableFormatters"

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
  GPU = "gpu",
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
        onChange={table.getToggleAllRowsSelectedHandler()}
        size="small"
        sx={{ p: 0 }}
      />
    ),
    cell: ({ row }) => (
      <Checkbox
        checked={row.getIsGrouped() ? row.getIsAllSubRowsSelected() : row.getIsSelected()}
        indeterminate={row.getIsSomeSelected()}
        size="small"
        sx={{
          p: 0,
          ml: `${row.depth * 6}px`,
        }}
      />
    ),
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
      cell: (cell) => (
        <JobStateLabel state={cell.getValue() as JobState}>{formatJobState(cell.getValue() as JobState)}</JobStateLabel>
      ),
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
    accessor: "priority",
    displayName: "Priority",
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
    accessor: (jobTableRow) => formatCPU(jobTableRow.cpu),
    displayName: "CPUs",
  }),
  accessorColumn({
    id: StandardColumnId.Memory,
    accessor: (jobTableRow) => formatBytes(jobTableRow.memory),
    displayName: "Memory",
    additionalOptions: {
      size: 200,
    },
  }),
  accessorColumn({
    id: StandardColumnId.GPU,
    accessor: "gpu",
    displayName: "GPUs",
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

export const DEFAULT_COLUMN_MATCHES: Map<string, Match> = new Map([
  [StandardColumnId.Queue, Match.StartsWith],
  [StandardColumnId.JobSet, Match.StartsWith],
  [StandardColumnId.JobID, Match.Exact],
  [StandardColumnId.State, Match.AnyOf],
  [StandardColumnId.Owner, Match.StartsWith],
])

export const createAnnotationColumn = (annotationKey: string): JobTableColumn => {
  return accessorColumn({
    id: toAnnotationColId(annotationKey),
    accessor: (jobTableRow) => jobTableRow.annotations?.[annotationKey],
    displayName: annotationKey,
    additionalOptions: {
      enableColumnFilter: true,
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
