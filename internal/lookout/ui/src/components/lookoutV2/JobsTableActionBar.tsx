import { memo, useCallback, useMemo, useState } from "react"

import { Divider, Button } from "@mui/material"
import RefreshButton from "components/RefreshButton"
import ColumnSelect from "components/lookoutV2/ColumnSelect"
import GroupBySelect from "components/lookoutV2/GroupBySelect"
import { JobFilter } from "models/lookoutV2Models"
import { IGetJobsService } from "services/lookoutV2/GetJobsService"
import { UpdateJobsService } from "services/lookoutV2/UpdateJobsService"
import { ColumnId, createAnnotationColumn, JobTableColumn } from "utils/jobsTableColumns"

import { CancelDialog } from "./CancelDialog"
import styles from "./JobsTableActionBar.module.css"
import { ReprioritiseDialog } from "./ReprioritiseDialog"

export interface JobsTableActionBarProps {
  isLoading: boolean
  allColumns: JobTableColumn[]
  groupedColumns: ColumnId[]
  visibleColumns: ColumnId[]
  selectedItemFilters: JobFilter[][]
  onRefresh: () => void
  onColumnsChanged: (newColumns: JobTableColumn[]) => void
  toggleColumnVisibility: (columnId: ColumnId) => void
  onGroupsChanged: (newGroups: ColumnId[]) => void
  getJobsService: IGetJobsService
  updateJobsService: UpdateJobsService
}

export const JobsTableActionBar = memo(
  ({
    isLoading,
    allColumns,
    groupedColumns,
    visibleColumns,
    selectedItemFilters,
    onRefresh,
    onColumnsChanged,
    toggleColumnVisibility,
    onGroupsChanged,
    getJobsService,
    updateJobsService,
  }: JobsTableActionBarProps) => {
    const [cancelDialogOpen, setCancelDialogOpen] = useState(false)
    const [reprioritiseDialogOpen, setReprioritiseDialogOpen] = useState(false)

    const selectableColumns = useMemo(() => allColumns.filter((col) => col.enableHiding !== false), [allColumns])

    function addAnnotationColumn(name: string, existingColumns = allColumns) {
      const annotationCol = createAnnotationColumn(name)
      const newColumns = existingColumns.concat([annotationCol])
      onColumnsChanged(newColumns)
      toggleColumnVisibility(annotationCol.id as ColumnId)
    }

    function removeAnnotationColumn(key: ColumnId) {
      const filtered = allColumns.filter((col) => col.id !== key)
      onColumnsChanged(filtered)
      return filtered
    }

    function renameAnnotationColumn(key: ColumnId, newName: string) {
      const remainingCols = removeAnnotationColumn(key)
      addAnnotationColumn(newName, remainingCols)
    }

    const numSelectedItems = selectedItemFilters.length

    const cancelDialogOnClose = useCallback(() => setCancelDialogOpen(false), [])
    const reprioritiseDialogOnClose = useCallback(() => setReprioritiseDialogOpen(false), [])
    return (
      <div className={styles.actionBar}>
        {cancelDialogOpen && (
          <CancelDialog
            onClose={cancelDialogOnClose}
            selectedItemFilters={selectedItemFilters}
            getJobsService={getJobsService}
            updateJobsService={updateJobsService}
          />
        )}
        {reprioritiseDialogOpen && (
          <ReprioritiseDialog
            onClose={reprioritiseDialogOnClose}
            selectedItemFilters={selectedItemFilters}
            getJobsService={getJobsService}
            updateJobsService={updateJobsService}
          />
        )}
        <div className={styles.actionGroup}>
          <GroupBySelect columns={allColumns} groups={groupedColumns} onGroupsChanged={onGroupsChanged} />
        </div>

        <div className={styles.actionGroup}>
          <RefreshButton isLoading={isLoading} onClick={onRefresh} />
          <ColumnSelect
            selectableColumns={selectableColumns}
            groupedColumns={groupedColumns}
            visibleColumns={visibleColumns}
            onAddAnnotation={addAnnotationColumn}
            onToggleColumn={toggleColumnVisibility}
            onEditAnnotation={renameAnnotationColumn}
            onRemoveAnnotation={removeAnnotationColumn}
          />
          <Divider orientation="vertical" />
          <Button variant="contained" disabled={numSelectedItems === 0} onClick={() => setCancelDialogOpen(true)}>
            Cancel selected
          </Button>
          <Button variant="contained" disabled={numSelectedItems === 0} onClick={() => setReprioritiseDialogOpen(true)}>
            Reprioritize selected
          </Button>
        </div>
      </div>
    )
  },
)
