import { ArrowBack, ArrowForward, Clear } from "@mui/icons-material"
import {
  Divider,
  FormControl,
  IconButton,
  InputLabel,
  MenuItem,
  OutlinedInput,
  Select,
  styled,
  SvgIcon,
} from "@mui/material"

import { ColumnId, getColumnMetadata, JobTableColumn, toColId } from "../../utils/jobsTableColumns"

const DIVIDER_WIDTH = 10

const GroupBySelectContainer = styled("div")({
  display: "flex",
  flexDirection: "row",
  alignItems: "center",
})

const GroupColumnSelectDividerContainer = styled("div")({
  display: "flex",
  flexDirection: "row",
  alignItems: "center",
})

const GroupColumnReorderButtonsContainer = styled("div")({
  marginRight: DIVIDER_WIDTH,

  display: "flex",
  flexDirection: "row",
  justifyContent: "space-between",
})

interface GroupColumnSelectProps {
  columns: JobTableColumn[]
  groups: ColumnId[]
  currentlySelected: ColumnId | ""
  onSelect: (columnKey: ColumnId) => void
  onDelete: () => void
}
function GroupColumnSelect({ columns, currentlySelected, onSelect, onDelete }: GroupColumnSelectProps) {
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

  const swapGroupByColumns = (indexA: number, indexB: number) => {
    if (indexA < 0 || indexA >= groups.length || indexB < 0 || indexB >= groups.length) {
      console.error(
        `Invalid indexes provided for swappping group-by columns. Groups length: ${groups.length}. Index A: ${indexA}. Index B: ${indexB}.`,
      )
      return
    }

    const newGroups = [...groups]
    newGroups[indexA] = groups[indexB]
    newGroups[indexB] = groups[indexA]
    onGroupsChanged(newGroups)
  }

  return (
    <GroupBySelectContainer>
      {/* Controls to modify/remove selected groups */}
      {groups.map((key, i) => {
        const alreadyListed = groups.slice(0, i)
        const remainingOptions = groupableColumns.filter((c) => !alreadyListed.includes(toColId(c.id)))
        return (
          <div key={key}>
            <GroupColumnSelectDividerContainer>
              <GroupColumnSelect
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
              {remainingOptions.length > 1 && <Divider style={{ width: DIVIDER_WIDTH }} />}
            </GroupColumnSelectDividerContainer>
            {groups.length > 1 && (
              <GroupColumnReorderButtonsContainer>
                <IconButton size="small" disabled={i === 0} onClick={() => swapGroupByColumns(i - 1, i)}>
                  <ArrowBack fontSize="inherit" />
                </IconButton>
                <IconButton
                  size="small"
                  disabled={i === groups.length - 1}
                  onClick={() => swapGroupByColumns(i, i + 1)}
                >
                  <ArrowForward fontSize="inherit" />
                </IconButton>
              </GroupColumnReorderButtonsContainer>
            )}
          </div>
        )
      })}

      {/* Control for adding a new group */}
      {ungroupedColumns.length > 0 && (
        <div>
          <GroupColumnSelect
            key="new-group"
            columns={ungroupedColumns}
            groups={groups}
            currentlySelected={""}
            onSelect={(newKey) => {
              onGroupsChanged(groups.concat([newKey]))
            }}
            onDelete={() => null}
          />
          {groups.length > 1 && (
            <GroupColumnReorderButtonsContainer>
              <IconButton size="small" disabled hidden>
                <SvgIcon fontSize="inherit" />
              </IconButton>
            </GroupColumnReorderButtonsContainer>
          )}
        </div>
      )}
    </GroupBySelectContainer>
  )
}
