import {
  CODE_SNIPPETS_WRAP_LINES_KEY,
  JOB_RUN_LOGS_SHOW_TIMESTAMPS_KEY,
  JOB_RUN_LOGS_TEXT_SIZE_KEY,
  JOB_RUN_LOGS_WRAP_LINES_KEY,
} from "./localStorageKeys"
import {
  booleanFromStorageValue,
  booleanToStorageValue,
  textEnumFromStorageValue,
  textEnumToStorageValue,
  useLocalStorageValue,
} from "./storage"

export const JOB_RUN_LOGS_TEXT_SIZES = ["small", "medium", "large"] as const

export type JobRunLogsTextSize = (typeof JOB_RUN_LOGS_TEXT_SIZES)[number]

const jobRunLogsTextSizeEnumMap: Record<JobRunLogsTextSize, true> = {
  small: true,
  medium: true,
  large: true,
}

export const useJobRunLogsShowTimestamps = () =>
  useLocalStorageValue(JOB_RUN_LOGS_SHOW_TIMESTAMPS_KEY, false, booleanFromStorageValue, booleanToStorageValue)

export const useJobRunLogsWrapLines = () =>
  useLocalStorageValue(JOB_RUN_LOGS_WRAP_LINES_KEY, false, booleanFromStorageValue, booleanToStorageValue)

const jobRunLogsTextSizeFromStorageValue = (storageValue: string | null) =>
  textEnumFromStorageValue(storageValue, jobRunLogsTextSizeEnumMap)

export const useJobRunLogsTextSize = () =>
  useLocalStorageValue<JobRunLogsTextSize>(
    JOB_RUN_LOGS_TEXT_SIZE_KEY,
    "medium",
    jobRunLogsTextSizeFromStorageValue,
    textEnumToStorageValue,
  )

export const useCodeSnippetsWrapLines = () =>
  useLocalStorageValue(CODE_SNIPPETS_WRAP_LINES_KEY, true, booleanFromStorageValue, booleanToStorageValue)
