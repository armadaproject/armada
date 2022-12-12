import { Select, OutlinedInput, MenuItem, Checkbox, ListItemText, Box } from "@mui/material"
import { DebouncedTextField } from "components/lookoutV2/DebouncedTextField"
import { FilterType } from "utils/jobsTableColumns"

export interface JobsTableFilterProps {
  currentFilter?: string | string[]
  filterType: FilterType
  enumFilterValues?: EnumFilterOption[]
  id: string
  onFilterChange: (newFilter: string | string[] | undefined) => void
}
export const JobsTableFilter = ({
  currentFilter,
  filterType,
  enumFilterValues,
  onFilterChange,
}: JobsTableFilterProps) => {
  return (
    <Box sx={{ display: "block", width: "100%" }}>
      {filterType === FilterType.Enum ? (
        <EnumFilter
          currentFilter={(currentFilter ?? []) as string[]}
          enumFilterValues={enumFilterValues ?? []}
          onFilterChange={onFilterChange}
        />
      ) : (
        <TextFilter currentFilter={(currentFilter ?? "") as string} onFilterChange={onFilterChange} />
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
  onFilterChange: JobsTableFilterProps["onFilterChange"]
}
const EnumFilter = ({ currentFilter, enumFilterValues, onFilterChange }: EnumFilterProps) => {
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
          <div style={{ color: "rgba(0, 0, 0, 0.3)" }}>Filter...</div>
        )
      }
      // Matches the styling for TextFilter component below
      sx={{
        width: "100%",
        height: "1.5em",
      }}
      SelectDisplayProps={{
        "aria-label": "Filter",
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
  onFilterChange: JobsTableFilterProps["onFilterChange"]
}
const TextFilter = ({ currentFilter, onFilterChange }: TextFilterProps) => {
  return (
    <DebouncedTextField
      debounceWaitMs={300}
      debouncedOnChange={onFilterChange}
      textFieldProps={{
        type: "text",
        size: "small",
        defaultValue: currentFilter,

        placeholder: "Filter...",
        inputProps: {
          "aria-label": "Filter",
          sx: {
            padding: "3.5px 7px",
            height: "1em",
          },
        },
      }}
    />
  )
}
