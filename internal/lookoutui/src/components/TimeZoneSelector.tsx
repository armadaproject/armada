import { useMemo } from "react"

import { Autocomplete, AutocompleteProps, MenuItem, styled, TextField, Typography } from "@mui/material"

import {
  getBrowserTimeZone,
  getTimeZoneOffsetAtTimestamp,
  TIME_ZONE_NAMES,
  UTC_TIME_ZONE_NAME,
} from "../common/timeZones"
import { useGetUiConfig } from "../services/lookout/useGetUiConfig"
import { SPACING } from "../styling/spacing"

// Place the local time zone and UTC first
const FIRST_TIME_ZONES_TO_DISPLAY = [getBrowserTimeZone(), UTC_TIME_ZONE_NAME]

const now = new Date()

const MenuItemContentContainer = styled("div")(({ theme }) => ({
  display: "flex",
  gap: theme.spacing(SPACING.sm),
  justifyContent: "space-between",
  width: "100%",
}))

export interface TimeZoneSelectorProps {
  idPrefix: string
  label: string
  value: string
  onChange: (timeZone: string) => void
  fullWidth?: AutocompleteProps<string, false, true, false>["fullWidth"]
  size?: AutocompleteProps<string, false, true, false>["size"]
}

export const TimeZoneSelector = ({ idPrefix, label, value, onChange, fullWidth, size }: TimeZoneSelectorProps) => {
  const { data, status, error } = useGetUiConfig()
  const { topOptions, otherOptions } = useMemo(
    () => ({
      topOptions: [
        ...FIRST_TIME_ZONES_TO_DISPLAY,
        ...(data?.pinnedTimeZoneIdentifiers ?? []).filter((tz) => !FIRST_TIME_ZONES_TO_DISPLAY.includes(tz)),
      ],
      otherOptions: TIME_ZONE_NAMES.filter(
        (tz) => !FIRST_TIME_ZONES_TO_DISPLAY.includes(tz) && !(data?.pinnedTimeZoneIdentifiers ?? []).includes(tz),
      ),
    }),
    [data?.pinnedTimeZoneIdentifiers],
  )

  if (status === "error") {
    console.error("Error getting UI config", error)
  }

  return (
    <Autocomplete
      id={`${idPrefix}-tz-selector`}
      disablePortal
      options={[...topOptions, ...otherOptions]}
      loading={status === "pending"}
      renderInput={(params) => <TextField {...params} label={label} />}
      renderOption={(props, option) => {
        const { key, ...optionProps } = props
        const { abbreviation, offsetMinutes } = getTimeZoneOffsetAtTimestamp(option, now)

        // Invert the offset when displaying it
        const absOffsetHoursComponent = Math.floor(Math.abs(offsetMinutes / 60))
        const absOffsetMinutesComponent = Math.abs(offsetMinutes % 60)
        const offsetDisplayString = `${offsetMinutes > 0 ? "-" : "+"}${absOffsetHoursComponent
          .toString()
          .padStart(2, "0")}:${absOffsetMinutesComponent.toString().padStart(2, "0")}`

        return (
          <MenuItem key={key} {...optionProps} divider={option === topOptions[topOptions.length - 1]}>
            <MenuItemContentContainer>
              <Typography component="div">
                {option}
                {abbreviation && (
                  <>
                    {" "}
                    <Typography component="span" variant="body2" color="text.secondary">
                      ({abbreviation})
                    </Typography>
                  </>
                )}
              </Typography>
              <Typography component="div" color="text.secondary">
                {option === getBrowserTimeZone() && (
                  <>
                    <em>Local</em>{" "}
                  </>
                )}
                {offsetDisplayString}
              </Typography>
            </MenuItemContentContainer>
          </MenuItem>
        )
      }}
      fullWidth={fullWidth}
      size={size}
      disableClearable
      value={value}
      onChange={(_, newValue) => onChange(newValue)}
    />
  )
}
