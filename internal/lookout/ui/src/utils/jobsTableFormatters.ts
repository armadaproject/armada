import { cyan, green, grey, orange, pink, purple, red, yellow } from "@mui/material/colors"
import { intervalToDuration } from "date-fns"
import { formatInTimeZone } from "date-fns-tz"
import { parseISO } from "date-fns/fp"
import { JobRunState, jobRunStateDisplayInfo, JobState, jobStateDisplayInfo } from "models/lookoutV2Models"

export const formatJobState = (state?: JobState): string =>
  state !== undefined ? jobStateDisplayInfo[state]?.displayName ?? state : ""

export const formatJobRunState = (state?: JobRunState): string =>
  state !== undefined ? jobRunStateDisplayInfo[state]?.displayName ?? state : ""

export const colorForJobState = (state?: JobState): string | undefined => {
  switch (state) {
    case JobState.Queued:
      return yellow["A100"]
    case JobState.Pending:
      return orange["A100"]
    case JobState.Running:
      return green["A100"]
    case JobState.Succeeded:
      return undefined
    case JobState.Failed:
      return red["A100"]
    case JobState.Cancelled:
      return grey[300]
    case JobState.Preempted:
      return pink[100]
    case JobState.Leased:
      return cyan[100]
    default:
      return purple["A100"]
  }
}

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
