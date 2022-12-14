import { formatDistanceStrict } from "date-fns"
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

export const formatTimeSince = (date?: string, now = Date.now()): string =>
  date !== undefined ? formatDistanceStrict(parseISO(date), now) : ""
