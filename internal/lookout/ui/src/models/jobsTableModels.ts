import { RowId } from "utils/reactTableUtils"

import { Job } from "./lookoutV2Models"

export interface BaseJobTableRow {
  rowId: RowId
}

export type JobRow = BaseJobTableRow & Partial<Job>
export type JobGroupRow = BaseJobTableRow & {
  [groupedField in keyof Job]?: Job[groupedField]
} & {
  isGroup: true // The ReactTable version of this doesn't seem to play nice with manual/serverside expanding
  jobCount?: number

  subRowCount?: number
  subRows: JobTableRow[]
  groupedField: string
}

export type JobTableRow = JobRow | JobGroupRow

export const isJobGroupRow = (row?: JobTableRow): row is JobGroupRow => row !== undefined && "isGroup" in row
