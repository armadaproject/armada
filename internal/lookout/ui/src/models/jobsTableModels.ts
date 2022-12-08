import { RowId } from "utils/reactTableUtils"

export interface BaseJobTableRow {
  rowId: RowId
}

export interface JobRow extends BaseJobTableRow {
  // Job details
  jobId?: string
  jobSet?: string
  queue?: string
  state?: string
  cpu?: number
  memory?: string
  ephemeralStorage?: string
}

export interface JobGroupRow extends BaseJobTableRow {
  isGroup: true // The ReactTable version of this doesn't seem to play nice with manual/serverside expanding
  jobCount?: number

  subRowCount?: number
  subRows: JobTableRow[]

  // Some subfield of JobRow that this row is grouped on
  [groupedField: string]: unknown
  groupedField: string
}

export type JobTableRow = JobRow | JobGroupRow

export const isJobGroupRow = (row?: JobTableRow): row is JobGroupRow => row !== undefined && "isGroup" in row
