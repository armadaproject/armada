import React, { RefObject, useEffect, useRef, useState } from "react"

import MoreVert from "@material-ui/icons/MoreVert"
import { Check } from "@mui/icons-material"
import { Box, Checkbox, IconButton, InputAdornment, ListItemText, MenuItem, OutlinedInput, Select } from "@mui/material"
import Menu from "@mui/material/Menu"
import { DebouncedTextField } from "components/lookoutV2/DebouncedTextField"
import { Match, MATCH_DISPLAY_STRINGS } from "models/lookoutV2Models"
import { ANNOTATION_COLUMN_PREFIX, FilterType, isStandardColId, VALID_COLUMN_MATCHES } from "utils/jobsTableColumns"

const ELLIPSIS = "\u2026"

const FILTER_TYPE_DISPLAY_STRINGS: Record<Match, string> = {
  [Match.Exact]: `Matches${ELLIPSIS}`,
  [Match.StartsWith]: `Starts with${ELLIPSIS}`,
  [Match.Contains]: `Contains${ELLIPSIS}`,
  [Match.GreaterThan]: `Greater than${ELLIPSIS}`,
  [Match.LessThan]: `Less than${ELLIPSIS}`,
  [Match.GreaterThanOrEqual]: `Greater than${ELLIPSIS}`,
  [Match.LessThanOrEqual]: `Less than${ELLIPSIS}`,
  [Match.AnyOf]: `Filter${ELLIPSIS}`,
  [Match.Exists]: `Annotation exists`,
}

export interface JobsTableFilterProps {
  currentFilter?: string | string[] | number
  filterType: FilterType
  matchType: Match
  enumFilterValues?: EnumFilterOption[]
  id: string
  parseError: string | undefined
  onFilterChange: (newFilter: string | string[] | number | undefined) => void
  onColumnMatchChange: (columnId: string, newMatch: Match) => void
  onSetTextFieldRef: (ref: RefObject<HTMLInputElement>) => void
}

export const JobsTableFilter = ({
  id,
  currentFilter,
  filterType,
  matchType,
  parseError,
  enumFilterValues,
  onFilterChange,
  onColumnMatchChange,
  onSetTextFieldRef,
}: JobsTableFilterProps) => {
  const label = FILTER_TYPE_DISPLAY_STRINGS[matchType]
  let possibleMatches = id in VALID_COLUMN_MATCHES ? VALID_COLUMN_MATCHES[id] : [Match.Exact]
  if (!isStandardColId(id)) {
    possibleMatches = VALID_COLUMN_MATCHES[ANNOTATION_COLUMN_PREFIX]
  }
  let filter = <></>
  if (filterType === FilterType.Enum) {
    filter = (
      <EnumFilter
        currentFilter={(currentFilter ?? []) as string[]}
        enumFilterValues={enumFilterValues ?? []}
        label={label}
        onFilterChange={onFilterChange}
      />
    )
  } else {
    filter = (
      <TextFilter
        defaultValue={(currentFilter as string | undefined) ?? ""}
        label={label}
        match={matchType}
        possibleMatches={possibleMatches}
        parseError={parseError}
        onChange={onFilterChange}
        onColumnMatchChange={(newMatch) => {
          onColumnMatchChange(id, newMatch)
        }}
        onSetTextFieldRef={onSetTextFieldRef}
      />
    )
  }
  return <Box sx={{ display: "block", width: "100%" }}>{filter}</Box>
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
  defaultValue: string
  label: string
  match: Match
  possibleMatches: Match[]
  parseError: string | undefined
  onChange: (newVal: string) => void
  onColumnMatchChange: (newMatch: Match) => void
  onSetTextFieldRef: (ref: RefObject<HTMLInputElement>) => void
}

const TextFilter = ({
  defaultValue,
  label,
  match,
  possibleMatches,
  parseError,
  onChange,
  onColumnMatchChange,
  onSetTextFieldRef,
}: TextFilterProps) => {
  const ref = useRef<HTMLInputElement>(null)
  useEffect(() => {
    onSetTextFieldRef(ref)
  }, [ref])
  return (
    <DebouncedTextField
      debouncedOnChange={onChange}
      debounceWaitMs={300}
      textFieldProps={{
        inputRef: ref,
        type: "text",
        size: "small",
        defaultValue: defaultValue,
        error: parseError !== undefined,
        placeholder: label,
        sx: {
          width: "100%",
        },
        inputProps: {
          "aria-label": label,
          sx: {
            padding: "3.5px 7px",
            height: "1em",
            width: "100%",
          },
        },
        InputProps: {
          style: {
            paddingRight: 0,
          },
          endAdornment: (
            <InputAdornment position="end">
              <MatchSelect possibleMatches={possibleMatches} currentMatch={match} onSelect={onColumnMatchChange} />
            </InputAdornment>
          ),
        },
      }}
    />
  )
}

interface MatchSelectProps {
  possibleMatches: Match[]
  currentMatch: Match
  onSelect: (newMatch: Match) => void
}

const MatchSelect = ({ possibleMatches, currentMatch, onSelect }: MatchSelectProps) => {
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null)
  const open = Boolean(anchorEl)
  const handleClick = (event: React.MouseEvent<HTMLButtonElement>) => {
    setAnchorEl(event.currentTarget)
  }
  const handleClose = () => {
    setAnchorEl(null)
  }
  return (
    <>
      <IconButton
        size="small"
        style={{
          height: "22px",
          width: "22px",
        }}
        onClick={handleClick}
      >
        <MoreVert />
      </IconButton>
      <Menu open={open} onClose={handleClose} anchorEl={anchorEl}>
        {possibleMatches.map((match, i) => {
          const matchStr = MATCH_DISPLAY_STRINGS[match]
          return (
            <MenuItem
              key={i}
              onClick={() => {
                onSelect(match)
                handleClose()
              }}
              style={{
                paddingLeft: "7px",
              }}
            >
              <div
                style={{
                  display: "flex",
                  flexDirection: "row",
                  alignItems: "center",
                  height: "100%",
                  gap: "5px",
                }}
              >
                <div
                  style={{
                    width: "25px",
                    height: "100%",
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                  }}
                >
                  {currentMatch === match && (
                    <Check
                      style={{
                        width: "20px",
                        height: "20px",
                      }}
                    />
                  )}
                </div>
                <div>{matchStr}</div>
              </div>
            </MenuItem>
          )
        })}
      </Menu>
    </>
  )
}
