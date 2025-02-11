import { intervalToDuration } from "date-fns"
import { parseISO } from "date-fns/fp"
import { formatInTimeZone } from "date-fns-tz"

import { JobRunState, jobRunStateDisplayInfo, JobState, jobStateDisplayNames } from "../models/lookoutV2Models"

export const formatJobState = (state?: JobState): string => (state ? (jobStateDisplayNames[state] ?? state) : "")

export const formatJobRunState = (state?: JobRunState): string =>
  state !== undefined ? (jobRunStateDisplayInfo[state]?.displayName ?? state) : ""

export const formatUtcDate = (date?: string): string => {
  if (date !== undefined) {
    try {
      return formatInTimeZone(parseISO(date), "UTC", "yyyy-MM-dd HH:mm")
    } catch (e) {
      console.warn("Failed to format date as UTC", date, e)
    }
  }

  return ""
}

export const formatTimeSince = (date?: string, now = Date.now()): string => {
  if (date === undefined || date.length === 0) {
    return ""
  }

  try {
    const duration = intervalToDuration({
      start: parseISO(date),
      end: now,
    })

    const difference = now - parseISO(date).getTime()
    if (difference < 0) {
      return ""
    }
    const days = Math.floor(difference / (1000 * 3600 * 24))
    const denominations = [
      { symbol: "d", value: days ?? 0 },
      { symbol: "h", value: duration.hours ?? 0 },
      { symbol: "m", value: duration.minutes ?? 0 },
      { symbol: "s", value: duration.seconds ?? 0 },
    ]

    return denominations
      .filter((d) => d.value !== 0)
      .map((d) => `${d.value}${d.symbol}`)
      .join(" ")
  } catch (e) {
    console.warn("Failed to format date as TimeSince", date, e)
    return ""
  }
}
