import { JobRunState, jobRunStateDisplayInfo, JobState, jobStateDisplayNames } from "../models/lookoutModels"

export const formatJobState = (state?: JobState): string => (state ? (jobStateDisplayNames[state] ?? state) : "")

export const formatJobRunState = (state?: JobRunState): string =>
  state !== undefined ? (jobRunStateDisplayInfo[state]?.displayName ?? state) : ""
