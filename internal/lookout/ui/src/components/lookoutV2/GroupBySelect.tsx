import { Fragment } from "react"

import { Clear } from "@mui/icons-material"
import { Divider, FormControl, IconButton, InputLabel, MenuItem, OutlinedInput, Select } from "@mui/material"
import { ColumnId, getColumnMetadata, JobTableColumn, toColId } from "utils/jobsTableColumns"

import styles from "./GroupBySelect.module.css"

interface GroupColumnProps {
  columns: JobTableColumn[]
  groups: ColumnId[]
  currentlySelected: ColumnId | ""
  onSelect: (columnKey: ColumnId) => void
  onDelete: () => void
}
function GroupColumn({ columns, currentlySelected, onSelect, onDelete }: GroupColumnProps) {
  const isGrouped = currentlySelected !== ""
  const actionText = isGrouped ? "Grouped by" : "Group by"
  const labelId = `select-column-group-${currentlySelected}`
  return (
    <FormControl size="small" focused={false} sx={{ mt: "4px" }}>
      <InputLabel id={labelId} size="small">
        {actionText}
      </InputLabel>
      <Select
        labelId={labelId}
        value={currentlySelected}
        size="small"
        sx={{
          minWidth: 110,
          paddingRight: "0.5em",

          // Only show the dropdown arrow if the clear icon isn't being shown
          "& .MuiSelect-iconOutlined": { display: isGrouped ? "none" : "" },
        }}
        input={<OutlinedInput label={actionText} />}
        endAdornment={
          isGrouped && (
            <IconButton size="small" sx={{ padding: 0 }} onClick={onDelete}>
              <Clear aria-label="Clear grouping" aria-hidden="false" />
            </IconButton>
          )
        }
      >
        {columns.map((col) => (
          <MenuItem
            key={col.id}
            value={col.id}
            disabled={currentlySelected === col.id}
            onClick={() => onSelect(toColId(col.id))}
          >
            {getColumnMetadata(col).displayName}
          </MenuItem>
        ))}
      </Select>
    </FormControl>
  )
}

export interface GroupBySelectProps {
  groups: ColumnId[]
  columns: JobTableColumn[]
  onGroupsChanged: (newGroups: ColumnId[]) => void
}
export default function GroupBySelect({ groups, columns, onGroupsChanged }: GroupBySelectProps) {
  const groupableColumns = columns.filter((col) => col.enableGrouping)
  const ungroupedColumns = groupableColumns.filter((c) => !groups.includes(toColId(c.id)))
  return (
    <div className={styles.container}>
      {/* Controls to modify/remove selected groups */}
      {groups.map((key, i) => {
        const alreadyListed = groups.slice(0, i)
        const remainingOptions = groupableColumns.filter((c) => !alreadyListed.includes(toColId(c.id)))
        return (
          <Fragment key={key}>
            <GroupColumn
              columns={remainingOptions}
              groups={groups}
              currentlySelected={key}
              onSelect={(newKey) => {
                // Resets everything to the right
                onGroupsChanged(alreadyListed.concat([newKey]))
              }}
              onDelete={() => {
                onGroupsChanged(groups.filter((_, idx) => idx !== i))
              }}
            />
            {remainingOptions.length > 1 && <Divider style={{ width: 10 }} />}
          </Fragment>
        )
      })}

      {/* Control for adding a new group */}
      {ungroupedColumns.length > 0 && (
        <GroupColumn
          key="new-group"
          columns={ungroupedColumns}
          groups={groups}
          currentlySelected={""}
          onSelect={(newKey) => {
            onGroupsChanged(groups.concat([newKey]))
          }}
          onDelete={() => null}
        />
      )}
    </div>
  )
}
