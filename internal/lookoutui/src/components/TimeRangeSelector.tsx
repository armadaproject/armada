import { useState } from "react"

import { alpha, Box, Button, Grid, MenuItem, MenuList, Popover, Stack, styled, Typography } from "@mui/material"
import { DateTimePicker } from "@mui/x-date-pickers"
import dayjs, { Dayjs } from "dayjs"

import { validDateFromNullableIsoString } from "../common/dates"
import { SPACING } from "../common/spacing"
import { useFormatTimestampTimeZone } from "../userSettings"

import {
  useDisplayedTimeZoneWithUserSettings,
  useFormatIsoTimestampWithUserSettings,
} from "./hooks/formatTimeWithUserSettings"

const ELLIPSIS = "\u2026"

const StyledButton = styled(Button, { shouldForwardProp: (propName) => propName !== "filterActive" })<{
  filterActive: boolean
}>(({ theme, filterActive }) => ({
  padding: "3.5px 7px",
  lineHeight: 1,
  fontSize: theme.typography.body1.fontSize,
  textTransform: "unset",
  margin: 0,
  borderColor: theme.palette.divider,
  color: filterActive ? theme.palette.text.primary : alpha(theme.palette.text.secondary, 0.5),
  fontWeight: filterActive ? theme.typography.fontWeightMedium : theme.typography.fontWeightRegular,
  justifyContent: filterActive ? "center" : "start",
  textWrap: "wrap",

  "&:hover": {
    borderColor: theme.palette.text.primary,
  },
}))

const PopoverGridContainer = styled(Grid)({
  width: "60ch",
})

interface PresetTimeRangeOption {
  displayName: string
  getStartIsoString: () => string | null
  getEndIsoString: () => string | null
}

const presetTimeRangeOptions: PresetTimeRangeOption[] = [
  {
    displayName: "Last 5 minutes",
    getStartIsoString: () => new Date(Date.now() - 5 * 60 * 1_000).toISOString(),
    getEndIsoString: () => null,
  },
  {
    displayName: "Last 15 minutes",
    getStartIsoString: () => new Date(Date.now() - 15 * 60 * 1_000).toISOString(),
    getEndIsoString: () => null,
  },
  {
    displayName: "Last 30 minutes",
    getStartIsoString: () => new Date(Date.now() - 30 * 60 * 1_000).toISOString(),
    getEndIsoString: () => null,
  },
  {
    displayName: "Last hour",
    getStartIsoString: () => new Date(Date.now() - 60 * 60 * 1_000).toISOString(),
    getEndIsoString: () => null,
  },
  {
    displayName: "Last 3 hours",
    getStartIsoString: () => new Date(Date.now() - 3 * 60 * 60 * 1_000).toISOString(),
    getEndIsoString: () => null,
  },
  {
    displayName: "Last 6 hours",
    getStartIsoString: () => new Date(Date.now() - 6 * 60 * 60 * 1_000).toISOString(),
    getEndIsoString: () => null,
  },
  {
    displayName: "Last 24 hours",
    getStartIsoString: () => new Date(Date.now() - 24 * 60 * 60 * 1_000).toISOString(),
    getEndIsoString: () => null,
  },
  {
    displayName: "Last 2 days",
    getStartIsoString: () => new Date(Date.now() - 2 * 24 * 60 * 60 * 1_000).toISOString(),
    getEndIsoString: () => null,
  },
  {
    displayName: "Last 7 days",
    getStartIsoString: () => new Date(Date.now() - 7 * 24 * 60 * 60 * 1_000).toISOString(),
    getEndIsoString: () => null,
  },
  {
    displayName: "Last 30 days",
    getStartIsoString: () => new Date(Date.now() - 30 * 24 * 60 * 60 * 1_000).toISOString(),
    getEndIsoString: () => null,
  },
]

export interface TimeRange {
  startIsoString: string | null
  endIsoString: string | null
}

export interface TimeRangeSelectorProps {
  value: TimeRange
  onChange: (timeRange: TimeRange) => void
}

export const TimeRangeSelector = ({ value: { startIsoString, endIsoString }, onChange }: TimeRangeSelectorProps) => {
  const formatIsoTimestamp = useFormatIsoTimestampWithUserSettings()
  const [timezone] = useFormatTimestampTimeZone()
  const displayedTimeZoneName = useDisplayedTimeZoneWithUserSettings()
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null)

  const [dateTimeSelectorKey, setDateTimeSelectorKey] = useState(0)

  const handleClose = () => {
    setAnchorEl(null)
  }

  const filterActive = Boolean(startIsoString || endIsoString)

  const startDate = validDateFromNullableIsoString(startIsoString)
  const endDate = validDateFromNullableIsoString(endIsoString)

  const buttonText = (() => {
    let startDateText: string | null = null
    let endDateText: string | null = null

    if (startIsoString && startDate) {
      startDateText = formatIsoTimestamp(startIsoString, "compact")
    }

    if (endIsoString && endDate) {
      endDateText = formatIsoTimestamp(endIsoString, "compact")
    }

    if (startDateText && endDateText) {
      return `${startDateText} - ${endDateText}`
    }

    if (startDateText) {
      return `After ${startDateText}`
    }

    if (endDateText) {
      return `Before ${endDateText}`
    }

    return `Set time range${ELLIPSIS}`
  })()

  const now = dayjs()

  return (
    <>
      <StyledButton
        variant="outlined"
        size="small"
        fullWidth
        color="inherit"
        filterActive={filterActive}
        onClick={({ currentTarget }) => setAnchorEl(currentTarget)}
      >
        {buttonText}
      </StyledButton>
      <Popover
        anchorEl={anchorEl}
        open={Boolean(anchorEl)}
        onClose={handleClose}
        anchorOrigin={{ vertical: "bottom", horizontal: "left" }}
      >
        <div>
          <PopoverGridContainer container>
            <Grid size={7} p={SPACING.sm}>
              <div>
                <DateTimePicker
                  key={dateTimeSelectorKey}
                  label="From"
                  timezone={timezone === "Etc/UTC" ? "UTC" : timezone}
                  views={["year", "month", "day", "hours", "minutes", "seconds"]}
                  slotProps={{
                    actionBar: { actions: ["cancel", "clear", "accept"] },
                    textField: { size: "small", margin: "dense", fullWidth: true },
                  }}
                  defaultValue={startDate ? dayjs(startDate) : undefined}
                  onAccept={(value: Dayjs | null) =>
                    onChange({ startIsoString: value?.toISOString() ?? null, endIsoString })
                  }
                  maxDateTime={endDate ? dayjs(endDate) : now}
                />
              </div>
              <div>
                <DateTimePicker
                  key={dateTimeSelectorKey}
                  label="To"
                  timezone={timezone === "Etc/UTC" ? "UTC" : timezone}
                  views={["year", "month", "day", "hours", "minutes", "seconds"]}
                  slotProps={{
                    actionBar: { actions: ["cancel", "today", "clear", "accept"] },
                    textField: { size: "small", margin: "dense", fullWidth: true },
                  }}
                  defaultValue={endDate ? dayjs(endDate) : undefined}
                  onAccept={(value: Dayjs | null) =>
                    onChange({ startIsoString, endIsoString: value?.toISOString() ?? null })
                  }
                  minDateTime={startDate ? dayjs(startDate) : undefined}
                  maxDateTime={now}
                />
              </div>
            </Grid>
            <Grid size={5} borderLeft={(theme) => `1px solid ${theme.palette.divider}`}>
              <MenuList dense>
                {presetTimeRangeOptions.map(({ displayName, getStartIsoString, getEndIsoString }) => (
                  <MenuItem
                    key={displayName}
                    onClick={() => {
                      onChange({ startIsoString: getStartIsoString(), endIsoString: getEndIsoString() })
                      handleClose()
                    }}
                  >
                    {displayName}
                  </MenuItem>
                ))}
              </MenuList>
            </Grid>
            <Grid size={12} p={SPACING.sm} borderTop={(theme) => `1px solid ${theme.palette.divider}`}>
              <Stack direction="row" spacing={SPACING.xs} alignContent="center">
                <div>
                  <Typography component="p" variant="body2">
                    Times are in <strong>{displayedTimeZoneName}</strong> ({timezone}).
                  </Typography>
                  <Typography component="p" variant="body2" color="text.secondary">
                    You can change this on the settings page.
                  </Typography>
                </div>
                <Box flex={1} />
                <Box alignContent="center">
                  <Button
                    variant="outlined"
                    size="small"
                    onClick={() => {
                      setDateTimeSelectorKey((prev) => prev + 1)
                      onChange({ startIsoString: null, endIsoString: null })
                    }}
                  >
                    Clear filter
                  </Button>
                </Box>
                <Box alignContent="center">
                  <Button size="small" onClick={handleClose}>
                    Close
                  </Button>
                </Box>
              </Stack>
            </Grid>
          </PopoverGridContainer>
        </div>
      </Popover>
    </>
  )
}
