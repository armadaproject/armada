import { EnumFilterOption } from "components/lookoutV2/JobsTableFilter"
import { capitalize } from "lodash"
import { formatJobState, Job, JobState, jobStateDisplayInfo } from "models/lookoutV2Models"
import prettyBytes from "pretty-bytes"

export type ColumnId = keyof Job | "selectorCol"

// TODO: Remove ColumnSpec indrection and just use Tanstack's ColumnDef?
export type ColumnSpec = {
  key: ColumnId
  name: string
  selected: boolean
  isAnnotation: boolean
  groupable: boolean
  sortable: boolean
  filterType?: FilterType
  enumFitlerValues?: EnumFilterOption[]
  minSize: number
  isNumeric?: boolean
  formatter?: (value: unknown) => string
}

export enum FilterType {
  Text = "Text",
  Enum = "Enum",
}

const getDefaultColumnSpec = (colId: ColumnId): ColumnSpec => ({
  key: colId,
  name: capitalize(colId),
  selected: true,
  isAnnotation: false,
  groupable: false,
  sortable: false,
  minSize: 30,
})

const numFormatter = Intl.NumberFormat()

const COLUMN_SPECS: ColumnSpec[] = [
  {
    key: "jobId",
    name: "Job Id",
    selected: true,
    isAnnotation: false,
    groupable: false,
    sortable: true,
    filterType: FilterType.Text,
    minSize: 100,
  },
  {
    key: "jobSet",
    name: "Job Set",
    selected: true,
    isAnnotation: false,
    groupable: true,
    sortable: false,
    filterType: FilterType.Text,
    minSize: 100,
  },
  {
    key: "queue",
    name: "Queue",
    selected: true,
    isAnnotation: false,
    groupable: true,
    sortable: false,
    filterType: FilterType.Text,
    minSize: 95,
  },
  {
    key: "state",
    name: "State",
    selected: true,
    isAnnotation: false,
    groupable: true,
    sortable: false,
    filterType: FilterType.Enum,
    enumFitlerValues: Object.values(JobState).map((state) => ({
      value: state,
      displayName: formatJobState(state),
    })),
    minSize: 60,
    formatter: (state) => (state ? jobStateDisplayInfo[state as JobState].displayName : ""),
  },
  {
    key: "cpu",
    name: "CPU",
    selected: true,
    isAnnotation: false,
    groupable: false,
    sortable: false,
    minSize: 60,
    isNumeric: true,
    formatter: (cpu) => (cpu !== undefined ? numFormatter.format(Number(cpu) / 1000) : ""),
  },
  {
    key: "memory",
    name: "Memory",
    selected: true,
    isAnnotation: false,
    groupable: false,
    sortable: false,
    minSize: 70,
    formatter: (bytes) => (bytes !== undefined ? prettyBytes(Number(bytes)) : ""),
  },
  {
    key: "ephemeralStorage",
    name: "Eph. Storage",
    selected: true,
    isAnnotation: false,
    groupable: false,
    sortable: false,
    minSize: 95,
    formatter: (bytes) => (bytes !== undefined ? prettyBytes(Number(bytes)) : ""),
  },
]

export const DEFAULT_COLUMNS: ColumnId[] = ["queue", "jobSet", "jobId", "state", "cpu", "memory", "ephemeralStorage"]

export const DEFAULT_GROUPING: ColumnId[] = []

const COLUMN_SPEC_MAP = COLUMN_SPECS.reduce<Record<ColumnId, ColumnSpec>>((map, spec) => {
  map[spec.key] = spec
  return map
}, {} as Record<ColumnId, ColumnSpec>)

export const columnSpecFor = (columnId: ColumnId): ColumnSpec =>
  COLUMN_SPEC_MAP[columnId] ?? getDefaultColumnSpec(columnId)

export const DEFAULT_COLUMN_SPECS = DEFAULT_COLUMNS.map(columnSpecFor)
