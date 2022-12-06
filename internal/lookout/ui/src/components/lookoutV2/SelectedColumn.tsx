import { Checkbox as MuiCheckbox } from "@mui/material"
import { ColumnDef } from "@tanstack/react-table"
import { JobTableRow } from "models/jobsTableModels"
import { memo, useCallback, useMemo } from "react"
import { ColumnId } from "utils/jobsTableColumns"

const Checkbox = memo(MuiCheckbox)

export const SELECT_COLUMN_ID: ColumnId = "selectorCol"
export const getSelectedColumnDef = (): ColumnDef<JobTableRow> => {
  const fixedWidthPixels = 35
  return {
    id: SELECT_COLUMN_ID as ColumnId,
    minSize: fixedWidthPixels,
    size: fixedWidthPixels,
    maxSize: fixedWidthPixels,
    aggregatedCell: undefined,
    enableColumnFilter: false,
    enableSorting: false,
    header: ({ table }) => {
      return (
        <Checkbox
          checked={table.getIsAllRowsSelected()}
          indeterminate={table.getIsSomeRowsSelected()}
          onChange={table.getToggleAllRowsSelectedHandler()}
          size="small"
        />
      )
    },
    cell: ({ row }) => {
      return (
        <Checkbox
          checked={row.getIsGrouped() ? row.getIsAllSubRowsSelected() : row.getIsSelected()}
          indeterminate={row.getIsSomeSelected()}
          onChange={useCallback(row.getToggleSelectedHandler(), [row])}
          size="small"
          sx={useMemo(
            () => ({
              marginLeft: `${row.depth * 6}px`,
            }),
            [],
          )}
        />
      )
    },
  }
}
