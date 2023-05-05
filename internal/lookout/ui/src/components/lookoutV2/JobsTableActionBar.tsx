import { memo, useCallback, useMemo, useState } from "react"

import { Divider, Button } from "@mui/material"
import RefreshButton from "components/RefreshButton"
import ColumnSelect from "components/lookoutV2/ColumnSelect"
import GroupBySelect from "components/lookoutV2/GroupBySelect"
import { JobFilter } from "models/lookoutV2Models"
import { IGetJobsService } from "services/lookoutV2/GetJobsService"
import { UpdateJobsService } from "services/lookoutV2/UpdateJobsService"
import { ColumnId, JobTableColumn } from "utils/jobsTableColumns"

import { useCustomSnackbar } from "../../hooks/useCustomSnackbar"
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
  onAddAnnotationColumn: (annotationKey: string) => void
  onRemoveAnnotationColumn: (colId: ColumnId) => void
  onEditAnnotationColumn: (colId: ColumnId, annotationKey: string) => void
  toggleColumnVisibility: (columnId: ColumnId) => void
  onGroupsChanged: (newGroups: ColumnId[]) => void
  getJobsService: IGetJobsService
  updateJobsService: UpdateJobsService
  onClearFilters: () => void
}

export const JobsTableActionBar = memo(
  ({
    isLoading,
    allColumns,
    groupedColumns,
    visibleColumns,
    selectedItemFilters,
    onRefresh,
    onAddAnnotationColumn,
    onRemoveAnnotationColumn,
    onEditAnnotationColumn,
    toggleColumnVisibility,
    onGroupsChanged,
    getJobsService,
    updateJobsService,
    onClearFilters,
  }: JobsTableActionBarProps) => {
    const [cancelDialogOpen, setCancelDialogOpen] = useState(false)
    const [reprioritiseDialogOpen, setReprioritiseDialogOpen] = useState(false)
    const openSnackbar = useCustomSnackbar()

    const selectableColumns = useMemo(() => allColumns.filter((col) => col.enableHiding !== false), [allColumns])

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
          <Button variant="text" onClick={onClearFilters} color="secondary">
            Clear Filters
          </Button>
          <RefreshButton isLoading={isLoading} onClick={onRefresh} />
          <ColumnSelect
            selectableColumns={selectableColumns}
            groupedColumns={groupedColumns}
            visibleColumns={visibleColumns}
            onAddAnnotation={(annotationKey) => {
              try {
                onAddAnnotationColumn(annotationKey)
              } catch (e) {
                const err = e as Error
                console.error(err.message)
                openSnackbar(`Failed to create annotation column: ${err.message}`, "error")
              }
            }}
            onRemoveAnnotation={(columnId) => {
              try {
                onRemoveAnnotationColumn(columnId)
              } catch (e) {
                const err = e as Error
                console.error(err.message)
                openSnackbar(`Failed to remove annotation column: ${err.message}`, "error")
              }
            }}
            onEditAnnotation={(columnId, annotationKey) => {
              try {
                onEditAnnotationColumn(columnId, annotationKey)
              } catch (e) {
                const err = e as Error
                console.error(err.message)
                openSnackbar(`Failed to edit annotation column: ${err.message}`, "error")
              }
            }}
            onToggleColumn={toggleColumnVisibility}
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
