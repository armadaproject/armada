const SECOND = 1
const MINUTE = 60 * SECOND
const HOUR = 60 * MINUTE
const DAY = 24 * HOUR

// Formats like the Age column of `kubectl get events`, e.g. 26s, 5m, 2h30m, 3h, 2d.
export const formatEventAge = (timestamp: string | undefined, now: Date): string => {
  if (!timestamp) {
    return ""
  }
  const time = new Date(timestamp).getTime()
  if (Number.isNaN(time)) {
    return ""
  }

  const seconds = Math.max(0, Math.floor((now.getTime() - time) / 1000))
  if (seconds < MINUTE) {
    return `${seconds}s`
  }
  if (seconds < HOUR) {
    return `${Math.floor(seconds / MINUTE)}m`
  }
  if (seconds < DAY) {
    const hours = Math.floor(seconds / HOUR)
    const minutes = Math.floor((seconds % HOUR) / MINUTE)
    return minutes > 0 ? `${hours}h${minutes}m` : `${hours}h`
  }
  return `${Math.floor(seconds / DAY)}d`
}
