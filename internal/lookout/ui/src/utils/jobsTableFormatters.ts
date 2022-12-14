import { intervalToDuration } from "date-fns"
import { formatInTimeZone } from "date-fns-tz"
import { parseISO } from "date-fns/fp"
import { JobState, jobStateDisplayInfo } from "models/lookoutV2Models"
import prettyBytes from "pretty-bytes"
const numFormatter = Intl.NumberFormat()

export const formatCPU = (cpuMillis?: number): string =>
  cpuMillis !== undefined ? numFormatter.format(cpuMillis / 1000) : ""

export const formatJobState = (state?: JobState): string =>
  state !== undefined ? jobStateDisplayInfo[state]?.displayName ?? state : ""

export const formatBytes = (bytes?: number): string => (bytes !== undefined ? prettyBytes(bytes) : "")

export const formatUtcDate = (date?: string): string =>
  date !== undefined ? formatInTimeZone(parseISO(date), "UTC", "yyyy-MM-dd HH:mm") : ""

export const formatTimeSince = (date?: string, now = Date.now()): string => {
  if (date === undefined || date.length === 0) {
    return ""
  }

  const duration = intervalToDuration({
    start: parseISO(date),
    end: now
  })

  const denominations = [
    {symbol: "y", value: duration.years ?? 0},
    {symbol: "w", value: duration.weeks ?? 0},
    {symbol: "d", value: duration.days ?? 0},
    {symbol: "h", value: duration.hours ?? 0},
    {symbol: "m", value: duration.minutes ?? 0},
    {symbol: "s", value: duration.seconds ?? 0},
  ]

  return denominations.filter(d => d.value !== 0).map(d => `${d.value}${d.symbol}`).join(" ")
}