import { JobRunState, jobRunStateDisplayInfo, JobState, jobStateDisplayNames } from "../models/lookoutModels"

export const formatJobState = (state?: JobState): string => (state ? (jobStateDisplayNames[state] ?? state) : "")

export const formatJobRunState = (state?: JobRunState): string =>
  state !== undefined ? (jobRunStateDisplayInfo[state]?.displayName ?? state) : ""

export const formatColumnList = (displayNames: string[]): string =>
  displayNames.length < 2
    ? (displayNames[0] ?? "")
    : `${displayNames.slice(0, -1).join(", ")} and ${displayNames[displayNames.length - 1]}`
