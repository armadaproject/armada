import { useCallback, useMemo } from "react"

import { formatTimestamp, TimestampFormat } from "../common/formatTime"
import { tzName } from "../common/timeZones"
import { useFormatTimestampLocale, useFormatTimestampShouldFormat, useFormatTimestampTimeZone } from "../userSettings"

export const useFormatIsoTimestampWithUserSettings = () => {
  const [shouldFormatTimestamp] = useFormatTimestampShouldFormat()
  const [locale] = useFormatTimestampLocale()
  const [timeZone] = useFormatTimestampTimeZone()

  return useCallback(
    (isoTimestampString: string | undefined, format: TimestampFormat) =>
      !shouldFormatTimestamp || !isoTimestampString
        ? (isoTimestampString ?? "")
        : formatTimestamp(isoTimestampString, { locale, timeZone, format }),
    [shouldFormatTimestamp, locale, timeZone],
  )
}

export const useDisplayedTimeZoneWithUserSettings = () => {
  const [shouldFormatTimestamp] = useFormatTimestampShouldFormat()
  const [timeZone] = useFormatTimestampTimeZone()

  const formatTimestampTimeZoneName = useMemo(() => tzName(timeZone), [timeZone])

  return shouldFormatTimestamp ? formatTimestampTimeZoneName : "UTC"
}
