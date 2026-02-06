import { ElementType, MouseEvent, RefObject, useCallback, useEffect, useRef, useState } from "react"

import { Check, Clear, MoreVert } from "@mui/icons-material"
import {
  Box,
  Checkbox,
  IconButton,
  InputAdornment,
  InputLabel,
  ListItemIcon,
  ListItemText,
  ListSubheader,
  MenuItem,
  OutlinedInput,
  Select,
  SvgIconProps,
  TextField,
} from "@mui/material"
import Menu from "@mui/material/Menu"
import { useDebouncedCallback } from "use-debounce"

import { validDateFromNullableIsoString } from "../common/dates"
import {
  ANNOTATION_COLUMN_PREFIX,
  FilterType,
  isStandardColId,
  StandardColumnId,
  VALID_COLUMN_MATCHES,
} from "../common/jobsTableColumns"
import { Match, MATCH_DISPLAY_STRINGS } from "../models/lookoutModels"
import { CustomPaletteColorToken } from "../theme"

import { QueueFilter } from "./QueueFilter"
import { TimeRangeSelector, type TimeRange } from "./TimeRangeSelector"

const ELLIPSIS = "\u2026"

const FILTER_TYPE_DISPLAY_STRINGS: Record<Match, string> = {
  [Match.Exact]: `Matches${ELLIPSIS}`,
  [Match.StartsWith]: `Starts with${ELLIPSIS}`,
  [Match.Contains]: `Contains${ELLIPSIS}`,
  [Match.GreaterThan]: `Greater than${ELLIPSIS}`,
  [Match.LessThan]: `Less than${ELLIPSIS}`,
  [Match.GreaterThanOrEqual]: `Greater than or equal to${ELLIPSIS}`,
  [Match.LessThanOrEqual]: `Less than or equal to${ELLIPSIS}`,
  [Match.AnyOf]: `Filter${ELLIPSIS}`,
  [Match.Exists]: `Annotation exists`,
}

export interface JobsTableFilterProps {
  currentFilter?: string | string[] | number
  filterType: FilterType
  matchType: Match
  enumFilterValues?: EnumFilterOption[]
  enumFilterCategories?: EnumFilterCategory[]
  id: string
  parseError: string | undefined
  onFilterChange: (newFilter: string | string[] | number | undefined) => void
  onColumnMatchChange: (columnId: string, newMatch: Match) => void
  onSetTextFieldRef: (ref: RefObject<HTMLInputElement | undefined>) => void
}

export const JobsTableFilter = ({
  id,
  currentFilter,
  filterType,
  matchType,
  parseError,
  enumFilterValues,
  enumFilterCategories,
  onFilterChange,
  onColumnMatchChange,
  onSetTextFieldRef,
}: JobsTableFilterProps) => {
  const onTimeRangeFilterChange = useCallback(
    ({ startIsoString, endIsoString }: TimeRange) => {
      if (!startIsoString && !endIsoString) {
        onFilterChange(undefined)
      } else {
        onFilterChange([startIsoString ?? "", endIsoString ?? ""])
      }
    },
    [onFilterChange],
  )

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
        categories={enumFilterCategories}
        label={label}
        onFilterChange={onFilterChange}
      />
    )
  } else if (filterType === FilterType.DateTimeRange) {
    const timeRange: TimeRange = { startIsoString: null, endIsoString: null }

    if (Array.isArray(currentFilter) && currentFilter.length === 2) {
      const startDate = validDateFromNullableIsoString(currentFilter[0])
      if (startDate) {
        timeRange.startIsoString = startDate.toISOString()
      }

      const endDate = validDateFromNullableIsoString(currentFilter[1])
      if (endDate) {
        timeRange.endIsoString = endDate.toISOString()
      }
    }

    filter = <TimeRangeSelector value={timeRange} onChange={onTimeRangeFilterChange} />
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

export interface EnumFilterCategory {
  value: string
  displayName: string
}

export interface EnumFilterOption {
  value: string
  displayName: string
  Icon?: ElementType<SvgIconProps>
  iconColor?: CustomPaletteColorToken
  categories?: string[]
}
interface EnumFilterProps {
  currentFilter: string[]
  enumFilterValues: EnumFilterOption[]
  categories?: EnumFilterCategory[]
  label: string
  onFilterChange: JobsTableFilterProps["onFilterChange"]
}
const EnumFilter = ({ currentFilter, enumFilterValues, categories, label, onFilterChange }: EnumFilterProps) => {
  const handleCategoryClick = (categoryValue: string) => {
    const statesInCategory = enumFilterValues
      .filter((option) => option.categories?.includes(categoryValue))
      .map((option) => option.value)
    const allStatesInCategorySelected = statesInCategory.every((state) => currentFilter.includes(state))

    if (allStatesInCategorySelected) {
      const newFilter = currentFilter.filter((state) => !statesInCategory.includes(state))
      onFilterChange(newFilter.length === 0 ? undefined : newFilter)
    } else {
      const newFilter = [...new Set([...currentFilter, ...statesInCategory])]
      onFilterChange(newFilter)
    }
  }

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
          // Filter out any invalid values that might come from clicking category headers
          const validValues = value.filter((v) => enumFilterValues.some((option) => option.value === v))
          onFilterChange(validValues.length === 0 ? undefined : validValues)
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
          paddingRight: currentFilter.length > 0 ? "54px" : 0,
        },
      }}
      endAdornment={
        currentFilter.length > 0 && (
          <InputAdornment position="end" sx={{ position: "absolute", right: "24px" }}>
            <IconButton
              size="small"
              onClick={(e) => {
                e.stopPropagation()
                onFilterChange(undefined)
              }}
              aria-label="Clear filter"
              sx={{
                height: "22px",
                width: "22px",
              }}
            >
              <Clear fontSize="small" />
            </IconButton>
          </InputAdornment>
        )
      }
    >
      {[
        // Render category headers and their options below them allowing selecting/deselecting all in category
        ...(categories ?? []).flatMap(({ value, displayName }) => {
          const categoryOptions = enumFilterValues.filter((option) => option.categories?.includes(value))
          const statesInCategory = categoryOptions.map((option) => option.value)
          const allStatesInCategorySelected = statesInCategory.every((state) => currentFilter.includes(state))
          const someStatesInCategorySelected =
            !allStatesInCategorySelected && statesInCategory.some((state) => currentFilter.includes(state))

          return [
            <ListSubheader
              key={value}
              component="div"
              role="button"
              tabIndex={0}
              aria-label={`${allStatesInCategorySelected ? "Deselect" : "Select"} all ${displayName} states`}
              onMouseDown={(e) => {
                e.preventDefault()
                e.stopPropagation()
                handleCategoryClick(value)
              }}
              onKeyDown={(e) => {
                if (e.key === "Enter") {
                  e.preventDefault()
                  e.stopPropagation()
                  handleCategoryClick(value)
                }
              }}
              sx={{
                cursor: "pointer",
                display: "flex",
                "&:hover": {
                  backgroundColor: "action.hover",
                },
                "&:focus": {
                  backgroundColor: "action.hover",
                },
              }}
            >
              <Checkbox
                checked={allStatesInCategorySelected}
                indeterminate={someStatesInCategorySelected}
                size="small"
                tabIndex={-1}
                sx={{ padding: "3px", pointerEvents: "none" }}
              />
              <ListItemText
                primary={displayName}
                slotProps={{
                  primary: {
                    fontWeight: "bold",
                    color: "text.primary",
                  },
                }}
              />
            </ListSubheader>,
            ...categoryOptions.map((option) => (
              <MenuItem key={option.value} value={option.value} dense sx={{ pl: 5 }}>
                <Checkbox checked={currentFilter.indexOf(option.value) > -1} size="small" sx={{ padding: "3px" }} />
                <ListItemText primary={option.displayName} />
                {option.Icon && (
                  <ListItemIcon>
                    <option.Icon fontSize="inherit" color={option.iconColor ?? "inherit"} />
                  </ListItemIcon>
                )}
              </MenuItem>
            )),
          ]
        }),
        ...(enumFilterValues ?? [])
          .filter((option) => !categories?.some((cat) => option.categories?.includes(cat.value)))
          .map(({ value, displayName, Icon, iconColor }) => (
            <MenuItem key={value} value={value} dense>
              <Checkbox checked={currentFilter.indexOf(value) > -1} size="small" sx={{ padding: "3px" }} />
              <ListItemText primary={displayName} />
              {Icon && (
                <ListItemIcon>
                  <Icon fontSize="inherit" color={iconColor ?? "inherit"} />
                </ListItemIcon>
              )}
            </MenuItem>
          )),
      ]}
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
  onSetTextFieldRef: (ref: RefObject<HTMLInputElement | undefined>) => void
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
  const ref = useRef<HTMLInputElement>(undefined)
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
      onPaste={(e) => {
        e.preventDefault()

        const trimmedPastedText = e.clipboardData.getData("text/plain").trim()
        if (!ref.current || ref.current.selectionStart == null || ref.current.selectionEnd === null) {
          setTextFieldValue(trimmedPastedText)
          return
        }

        const textBefore = textFieldValue.substring(0, ref.current.selectionStart)
        const textAfter = textFieldValue.substring(ref.current.selectionEnd)

        setTextFieldValue(textBefore + trimmedPastedText + textAfter)
      }}
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
              {textFieldValue && (
                <IconButton
                  size="small"
                  onClick={() => setTextFieldValue("")}
                  aria-label="Clear filter"
                  sx={{
                    height: "22px",
                    width: "22px",
                  }}
                >
                  <Clear fontSize="small" />
                </IconButton>
              )}
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
