import { ElementType, MouseEvent, RefObject, useEffect, useRef, useState } from "react"

import { Check, MoreVert } from "@mui/icons-material"
import {
  Box,
  Checkbox,
  IconButton,
  InputAdornment,
  InputLabel,
  ListItemIcon,
  ListItemText,
  MenuItem,
  OutlinedInput,
  Select,
  SvgIconProps,
  TextField,
} from "@mui/material"
import Menu from "@mui/material/Menu"
import { useDebouncedCallback } from "use-debounce"

import { QueueFilter } from "./QueueFilter"
import { Match, MATCH_DISPLAY_STRINGS } from "../../models/lookoutModels"
import { CustomPaletteColorToken } from "../../theme/palette"
import {
  ANNOTATION_COLUMN_PREFIX,
  FilterType,
  isStandardColId,
  StandardColumnId,
  VALID_COLUMN_MATCHES,
} from "../../utils/jobsTableColumns"

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
  if (id === StandardColumnId.Queue) {
    return (
      <QueueFilter
        filterValue={currentFilter as string[] | undefined}
        onFilterChange={onFilterChange}
        parseError={parseError}
        onSetTextFieldRef={onSetTextFieldRef}
      />
    )
  }

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
  Icon?: ElementType<SvgIconProps>
  iconColor?: CustomPaletteColorToken
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
          <InputLabel>{label}</InputLabel>
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
      {(enumFilterValues ?? []).map(({ value, displayName, Icon, iconColor }) => (
        <MenuItem key={value} value={value} dense>
          <Checkbox checked={currentFilter.indexOf(value) > -1} size="small" sx={{ padding: "3px" }} />
          <ListItemText primary={displayName} />
          {Icon && (
            <ListItemIcon>
              <Icon fontSize="inherit" color={iconColor ?? "inherit"} />
            </ListItemIcon>
          )}
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
  const [textFieldValue, setTextFieldValue] = useState(defaultValue)
  useEffect(() => {
    setTextFieldValue(defaultValue)
  }, [defaultValue])

  const debouncedOnChange = useDebouncedCallback(onChange, 300)
  useEffect(() => {
    debouncedOnChange(textFieldValue)
  }, [textFieldValue, debouncedOnChange])

  return (
    <TextField
      onChange={(e) => setTextFieldValue(e.currentTarget.value)}
      value={textFieldValue}
      inputRef={ref}
      type={"text"}
      size={"small"}
      error={parseError !== undefined}
      placeholder={label}
      sx={{
        width: "100%",
      }}
      slotProps={{
        input: {
          style: {
            paddingRight: 0,
          },
          endAdornment: (
            <InputAdornment position="end">
              <MatchSelect possibleMatches={possibleMatches} currentMatch={match} onSelect={onColumnMatchChange} />
            </InputAdornment>
          ),
        },

        htmlInput: {
          "aria-label": label,
          sx: {
            padding: "3.5px 7px",
            height: "1em",
            width: "100%",
          },
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
  const handleClick = (event: MouseEvent<HTMLButtonElement>) => {
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
