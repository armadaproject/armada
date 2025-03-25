import { listTimeZones, findTimeZone, getUTCOffset } from "timezone-support"

export interface TimeZoneOffset {
  abbreviation?: string
  // For example, a time zone 3 hours ahead of UTC would have an offsetMinutes value of -180
  offsetMinutes: number
}

export const TIME_ZONE_NAMES = listTimeZones()
export const TIME_ZONE_NAMES_SET = new Set(TIME_ZONE_NAMES)

export const UTC_TIME_ZONE_NAME = "Etc/UTC"

export const getTimeZoneOffsetAtTimestamp = (timeZoneName: string, ts: Date): TimeZoneOffset => {
  try {
    const { abbreviation, offset } = getUTCOffset(ts, findTimeZone(timeZoneName))
    return { abbreviation, offsetMinutes: offset }
  } catch {
    // fall back to UTC
    const { abbreviation, offset } = getUTCOffset(ts, findTimeZone(UTC_TIME_ZONE_NAME))
    return { abbreviation, offsetMinutes: offset }
  }
}

export const getBrowserTimeZone = () => Intl.DateTimeFormat().resolvedOptions().timeZone

// If using this with user settings, consider using the useDisplayedTimeZoneWithUserSettings() hook instead
export const tzName = (timeZone: string) => {
  const concreteTimeZone = timeZone || getBrowserTimeZone()

  const { abbreviation, offsetMinutes } = getTimeZoneOffsetAtTimestamp(concreteTimeZone, new Date())

  if (abbreviation) {
    return abbreviation
  }

  const absOffsetHoursComponent = Math.floor(Math.abs(offsetMinutes / 60))
  const absOffsetMinutesComponent = Math.abs(offsetMinutes % 60)
  return `UTC ${offsetMinutes > 0 ? "-" : "+"}${absOffsetHoursComponent
    .toString()
    .padStart(2, "0")}:${absOffsetMinutesComponent.toString().padStart(2, "0")}`
}
