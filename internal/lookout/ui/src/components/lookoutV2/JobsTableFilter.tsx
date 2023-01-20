import { Select, OutlinedInput, MenuItem, Checkbox, ListItemText, Box } from "@mui/material"
import { DebouncedTextField } from "components/lookoutV2/DebouncedTextField"
import { Match } from "models/lookoutV2Models"
import { FilterType } from "utils/jobsTableColumns"

const FILTER_TYPE_DISPLAY_STRINGS: Record<Match, string> = {
  [Match.Exact]: "Matches...",
  [Match.StartsWith]: "Starts with...",
  [Match.GreaterThan]: "Greater than...",
  [Match.LessThan]: "Less than...",
  [Match.GreaterThanOrEqual]: "Greater than...",
  [Match.LessThanOrEqual]: "Less than...",
  [Match.AnyOf]: "Filter...",
}

export interface JobsTableFilterProps {
  currentFilter?: string | string[]
  filterType: FilterType
  matchType: Match
  enumFilterValues?: EnumFilterOption[]
  id: string
  onFilterChange: (newFilter: string | string[] | undefined) => void
}
export const JobsTableFilter = ({
  currentFilter,
  filterType,
  matchType,
  enumFilterValues,
  onFilterChange,
}: JobsTableFilterProps) => {
  const label = FILTER_TYPE_DISPLAY_STRINGS[matchType]
  return (
    <Box sx={{ display: "block", width: "100%" }}>
      {filterType === FilterType.Enum ? (
        <EnumFilter
          currentFilter={(currentFilter ?? []) as string[]}
          enumFilterValues={enumFilterValues ?? []}
          label={label}
          onFilterChange={onFilterChange}
        />
      ) : (
        <TextFilter currentFilter={(currentFilter ?? "") as string} label={label} onFilterChange={onFilterChange} />
      )}
    </Box>
  )
}

export interface EnumFilterOption {
  value: string
  displayName: string
}
interface EnumFilterProps {
  currentFilter: string[]
  enumFilterValues: EnumFilterOption[]
  label: string
  onFilterChange: JobsTableFilterProps["onFilterChange"]
}
const EnumFilter = ({ currentFilter, enumFilterValues, label, onFilterChange }: EnumFilterProps) => {
  return (
    <Select
      variant="standard"
      multiple
      value={currentFilter}
      onChange={(e) => {
        const value = e.target.value
        if (typeof value === "string") {
          onFilterChange(value === "" ? undefined : value)
        } else {
          onFilterChange(value.length === 0 ? undefined : value)
        }
      }}
      input={<OutlinedInput margin="dense" />}
      displayEmpty={true}
      renderValue={(selected) =>
        selected.length > 0 ? (
          selected.map((s) => enumFilterValues.find((v) => v.value === s)?.displayName ?? s).join(", ")
        ) : (
          // Approximately matches the styling for a text input's placeholder
          <div style={{ color: "rgba(0, 0, 0, 0.3)" }}>{label}</div>
        )
      }
      // Matches the styling for TextFilter component below
      sx={{
        width: "100%",
        height: "1.5em",
      }}
      SelectDisplayProps={{
        "aria-label": label,
        style: {
          padding: 0,
          paddingLeft: "7px",
        },
      }}
    >
      {(enumFilterValues ?? []).map((option) => (
        <MenuItem key={option.value} value={option.value} dense>
          <Checkbox checked={currentFilter.indexOf(option.value) > -1} size="small" sx={{ padding: "3px" }} />
          <ListItemText primary={option.displayName} />
        </MenuItem>
      ))}
    </Select>
  )
}

interface TextFilterProps {
  currentFilter: string
  label: string
  onFilterChange: JobsTableFilterProps["onFilterChange"]
}
const TextFilter = ({ currentFilter, label, onFilterChange }: TextFilterProps) => {
  return (
    <DebouncedTextField
      debounceWaitMs={300}
      debouncedOnChange={(newFilter) => onFilterChange(newFilter.trim())}
      textFieldProps={{
        type: "text",
        size: "small",
        defaultValue: currentFilter,

        placeholder: label,
        inputProps: {
          "aria-label": label,
          sx: {
            padding: "3.5px 7px",
            height: "1em",
          },
        },
      }}
    />
  )
}
