import { memo, useCallback, useState } from "react"

import { Divider, Button } from "@mui/material"
import ColumnSelect from "components/lookoutV2/ColumnSelect"
import GroupBySelect from "components/lookoutV2/GroupBySelect"
import { JobFilter } from "models/lookoutV2Models"
import { CancelJobsService } from "services/lookoutV2/CancelJobsService"
import GetJobsService from "services/lookoutV2/GetJobsService"
import { ColumnSpec, columnSpecFor, ColumnId } from "utils/jobsTableColumns"

import { CancelDialog } from "./CancelDialog"
import styles from "./JobsTableActionBar.module.css"

export interface JobsTableActionBarProps {
  allColumns: ColumnSpec[]
  groupedColumns: ColumnId[]
  selectedItemFilters: JobFilter[][]
  onColumnsChanged: (newColumns: ColumnSpec[]) => void
  onGroupsChanged: (newGroups: ColumnId[]) => void
  getJobsService: GetJobsService
  cancelJobsService: CancelJobsService
}
export const JobsTableActionBar = memo(
  ({
    allColumns,
    groupedColumns,
    selectedItemFilters,
    onColumnsChanged,
    onGroupsChanged,
    getJobsService,
    cancelJobsService,
  }: JobsTableActionBarProps) => {
    const [cancelDialogOpen, setCancelDialogOpen] = useState(false)

    function toggleColumn(key: string) {
      const newColumns = allColumns.map((col) => ({
        ...col,
        selected: col.key === key ? !col.selected : col.selected,
      }))
      onColumnsChanged(newColumns)
    }

    function addAnnotationColumn(name: string) {
      const newColumns = allColumns.concat([
        {
          ...columnSpecFor(name as ColumnId),
          isAnnotation: true,
        },
      ])
      onColumnsChanged(newColumns)
    }

    function removeAnnotationColumn(key: string) {
      const filtered = allColumns.filter((col) => !col.isAnnotation || col.key !== key)
      onColumnsChanged(filtered)
    }

    function editAnnotationColumn(key: string, newName: string) {
      const newColumns = allColumns.map((col) => ({
        ...col,
        name: col.key === key ? newName : col.name,
      }))
      onColumnsChanged(newColumns)
    }

    const numSelectedItems = selectedItemFilters.length

    const cancelDialogOnClose = useCallback(() => setCancelDialogOpen(false), [])
    return (
      <div className={styles.actionBar}>
        {cancelDialogOpen && (
          <CancelDialog
            onClose={cancelDialogOnClose}
            selectedItemFilters={selectedItemFilters}
            getJobsService={getJobsService}
            cancelJobsService={cancelJobsService}
          />
        )}
        <div className={styles.actionGroup}>
          <GroupBySelect columns={allColumns} groups={groupedColumns} onGroupsChanged={onGroupsChanged} />
        </div>

        <div className={styles.actionGroup}>
          <ColumnSelect
            allColumns={allColumns}
            groupedColumns={groupedColumns}
            onAddAnnotation={addAnnotationColumn}
            onToggleColumn={toggleColumn}
            onEditAnnotation={editAnnotationColumn}
            onRemoveAnnotation={removeAnnotationColumn}
          />
          <Divider orientation="vertical" />
          <Button variant="contained" disabled={numSelectedItems === 0} onClick={() => setCancelDialogOpen(true)}>
            Cancel selected
          </Button>
          <Button variant="contained" disabled={numSelectedItems === 0}>
            Reprioritize selected
          </Button>
        </div>
      </div>
    )
  },
)
