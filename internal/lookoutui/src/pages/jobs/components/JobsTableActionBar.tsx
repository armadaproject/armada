import { memo, useCallback, useMemo, useState } from "react"

import { Clear, FilterAltOff, ViewColumn } from "@mui/icons-material"
import { Divider, Button, Checkbox, FormControlLabel, FormGroup, Tooltip } from "@mui/material"

import { ColumnId, JobTableColumn, PINNED_COLUMNS, toColId } from "../../../common/jobsTableColumns"
import AutoRefreshToggle from "../../../components/AutoRefreshToggle"
import RefreshButton from "../../../components/RefreshButton"
import { JobFiltersWithExcludes } from "../../../models/lookoutModels"

import { CancelDialog } from "./CancelDialog"
import { ColumnConfigurationDialog } from "./ColumnConfigurationDialog"
import { CustomViewPicker } from "./CustomViewPicker"
import GroupBySelect from "./GroupBySelect"
import styles from "./JobsTableActionBar.module.css"
import { PreemptDialog } from "./PreemptDialog"
import { ReprioritizeDialog } from "./ReprioritizeDialog"

export interface JobsTableActionBarProps {
  isLoading: boolean
  allColumns: JobTableColumn[]
  groupedColumns: ColumnId[]
  filterColumns: ColumnId[]
  sortColumns: ColumnId[]
  visibleColumns: ColumnId[]
  columnOrder: ColumnId[]
  setColumnOrder: (columnOrder: ColumnId[]) => void
  selectedItemFilters: JobFiltersWithExcludes[]
  customViews: string[]
  activeJobSets: boolean
  onActiveJobSetsChanged: (newVal: boolean) => void
  onRefresh: () => void
  autoRefresh: boolean
  onAutoRefreshChange: ((autoRefresh: boolean) => void) | undefined
  onAddAnnotationColumn: (annotationKey: string) => void
  onRemoveAnnotationColumn: (colId: ColumnId) => void
  onEditAnnotationColumn: (colId: ColumnId, annotationKey: string) => void
  toggleColumnVisibility: (columnId: ColumnId) => void
  onGroupsChanged: (newGroups: ColumnId[]) => void
  onClearFilters: () => void
  onClearSorting: () => void
  customSortingApplied: boolean
  onClearGroups: () => void
  onAddCustomView: (name: string) => void
  onDeleteCustomView: (name: string) => void
  onLoadCustomView: (name: string) => void
}

export const JobsTableActionBar = memo(
  ({
    isLoading,
    allColumns,
    groupedColumns,
    filterColumns,
    sortColumns,
    visibleColumns,
    columnOrder,
    setColumnOrder,
    selectedItemFilters,
    customViews,
    activeJobSets,
    onActiveJobSetsChanged,
    onRefresh,
    autoRefresh,
    onAutoRefreshChange,
    onAddAnnotationColumn,
    onRemoveAnnotationColumn,
    onEditAnnotationColumn,
    toggleColumnVisibility,
    onGroupsChanged,
    onClearFilters,
    onClearSorting,
    customSortingApplied,
    onClearGroups,
    onAddCustomView,
    onDeleteCustomView,
    onLoadCustomView,
  }: JobsTableActionBarProps) => {
    const [columnConfigurationDialogOpen, setColumnConfigurationDialogOpen] = useState(false)
    const [cancelDialogOpen, setCancelDialogOpen] = useState(false)
    const [reprioritizeDialogOpen, setReprioritizeDialogOpen] = useState(false)
    const [preemptDialogOpen, setPreemptDialogOpen] = useState(false)

    const numberSelectedColumns = useMemo(() => {
      const visibleColumnsSet = new Set(visibleColumns)
      return allColumns.filter((col) => {
        const colId = toColId(col.id)
        return !PINNED_COLUMNS.includes(colId) && visibleColumnsSet.has(colId)
      }).length
    }, [allColumns, visibleColumns])

    const numSelectedItems = selectedItemFilters.length

    const columnConfigurationDialogOpenOnClose = useCallback(() => setColumnConfigurationDialogOpen(false), [])
    const cancelDialogOnClose = useCallback(() => setCancelDialogOpen(false), [])
    const reprioritizeDialogOnClose = useCallback(() => setReprioritizeDialogOpen(false), [])
    const preemptDialogOnClose = useCallback(() => setPreemptDialogOpen(false), [])
    return (
      <div className={styles.actionBar}>
        <ColumnConfigurationDialog
          open={columnConfigurationDialogOpen}
          onClose={columnConfigurationDialogOpenOnClose}
          allColumns={allColumns}
          groupedColumnIds={groupedColumns}
          filterColumnIds={filterColumns}
          sortColumnIds={sortColumns}
          visibleColumnIds={visibleColumns}
          columnOrderIds={columnOrder}
          setColumnOrder={setColumnOrder}
          toggleColumnVisibility={toggleColumnVisibility}
          onAddAnnotationColumn={onAddAnnotationColumn}
          onEditAnnotationColumn={onEditAnnotationColumn}
          onRemoveAnnotationColumn={onRemoveAnnotationColumn}
        />
        {cancelDialogOpen && <CancelDialog onClose={cancelDialogOnClose} selectedItemFilters={selectedItemFilters} />}
        {reprioritizeDialogOpen && (
          <ReprioritizeDialog onClose={reprioritizeDialogOnClose} selectedItemFilters={selectedItemFilters} />
        )}
        {preemptDialogOpen && (
          <PreemptDialog onClose={preemptDialogOnClose} selectedItemFilters={selectedItemFilters} />
        )}
        <div className={styles.actionGroup}>
          <GroupBySelect columns={allColumns} groups={groupedColumns} onGroupsChanged={onGroupsChanged} />
        </div>

        <div className={styles.actionGroup}>
          <FormGroup>
            <Tooltip title="Only display job sets with at least one active job.">
              <FormControlLabel
                control={
                  <Checkbox
                    checked={activeJobSets}
                    onChange={(e) => {
                      onActiveJobSetsChanged(e.target.checked)
                    }}
                  />
                }
                label="Active job sets only"
              />
            </Tooltip>
          </FormGroup>
          <Divider orientation="vertical" />
          {onAutoRefreshChange && (
            <AutoRefreshToggle autoRefresh={autoRefresh} onAutoRefreshChange={onAutoRefreshChange} />
          )}
          <div>
            <RefreshButton isLoading={isLoading} onClick={onRefresh} />
          </div>
          <Divider orientation="vertical" />
          <div>
            <Button
              variant="outlined"
              onClick={() => {
                onClearFilters()
                onClearSorting()
              }}
              color="primary"
              endIcon={<FilterAltOff />}
              disabled={filterColumns.length === 0 && !customSortingApplied}
            >
              Clear Filters and Sorting
            </Button>
          </div>
          <div>
            <Button
              variant="outlined"
              onClick={onClearGroups}
              color="primary"
              endIcon={<Clear />}
              disabled={groupedColumns.length === 0}
            >
              Clear Groups
            </Button>
          </div>
          <div>
            <Button
              variant="outlined"
              color="secondary"
              endIcon={<ViewColumn />}
              onClick={() => setColumnConfigurationDialogOpen(true)}
            >
              Configure columns ({numberSelectedColumns})
            </Button>
          </div>
          <div>
            <CustomViewPicker
              customViews={customViews}
              onAddCustomView={onAddCustomView}
              onDeleteCustomView={onDeleteCustomView}
              onLoadCustomView={onLoadCustomView}
            />
          </div>
          <Divider orientation="vertical" />
          <div>
            <Button variant="contained" disabled={numSelectedItems === 0} onClick={() => setCancelDialogOpen(true)}>
              Cancel selected
            </Button>
          </div>
          <div>
            <Button
              variant="contained"
              disabled={numSelectedItems === 0}
              onClick={() => setReprioritizeDialogOpen(true)}
            >
              Reprioritize selected
            </Button>
          </div>
          <div>
            <Button variant="contained" disabled={numSelectedItems === 0} onClick={() => setPreemptDialogOpen(true)}>
              Preempt selected
            </Button>
          </div>
        </div>
      </div>
    )
  },
)
