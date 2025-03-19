import { identity } from "lodash"

import {
  CODE_SNIPPETS_WRAP_LINES_KEY,
  FORMAT_NUMBER_LOCALE_KEY,
  FORMAT_NUMBER_NOTATION_KEY,
  FORMAT_NUMBER_SHOULD_FORMAT_KEY,
  FORMAT_TIMESTAMP_LOCALE_KEY,
  FORMAT_TIMESTAMP_SHOULD_FORMAT_KEY,
  FORMAT_TIMESTAMP_TIME_ZONE_KEY,
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
import { DEFAULT_NUMBER_NOTATION, NumberNotation } from "../common/formatNumber"
import { BROWSER_LOCALE, SupportedLocale } from "../common/locales"
import { TIME_ZONE_NAMES_SET, UTC_TIME_ZONE_NAME } from "../common/timeZones"

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

const supportedLocaleEnumMap: Record<SupportedLocale | typeof BROWSER_LOCALE, true> = {
  [BROWSER_LOCALE]: true,
  "en-US": true,
  "en-GB": true,
  "en-IN": true,
  "en-AU": true,
  fr: true,
  de: true,
  es: true,
  "es-MX": true,
  "pt-BR": true,
  it: true,
  "zh-CN": true,
  ru: true,
  ja: true,
  ar: true,
}

const supportedLocaleFromStorageValue = (storageValue: string | null) =>
  textEnumFromStorageValue(storageValue, supportedLocaleEnumMap)

export const useFormatTimestampShouldFormat = () =>
  useLocalStorageValue(FORMAT_TIMESTAMP_SHOULD_FORMAT_KEY, true, booleanFromStorageValue, booleanToStorageValue)

const fromTimeZoneStorageValue = (storageValue: string | null) =>
  storageValue !== null && TIME_ZONE_NAMES_SET.has(storageValue) ? storageValue : null

export const useFormatTimestampTimeZone = () =>
  useLocalStorageValue<string>(FORMAT_TIMESTAMP_TIME_ZONE_KEY, UTC_TIME_ZONE_NAME, fromTimeZoneStorageValue, identity)

export const useFormatTimestampLocale = () =>
  useLocalStorageValue<SupportedLocale | typeof BROWSER_LOCALE>(
    FORMAT_TIMESTAMP_LOCALE_KEY,
    BROWSER_LOCALE,
    supportedLocaleFromStorageValue,
    textEnumToStorageValue,
  )

export const useFormatNumberShouldFormat = () =>
  useLocalStorageValue(FORMAT_NUMBER_SHOULD_FORMAT_KEY, true, booleanFromStorageValue, booleanToStorageValue)

export const useFormatNumberLocale = () =>
  useLocalStorageValue<SupportedLocale | typeof BROWSER_LOCALE>(
    FORMAT_NUMBER_LOCALE_KEY,
    BROWSER_LOCALE,
    supportedLocaleFromStorageValue,
    textEnumToStorageValue,
  )

const numbeNotationEnumMap: Record<NumberNotation, true> = {
  standard: true,
  scientific: true,
  engineering: true,
  compact: true,
}

const numberNotationFromStorageValue = (storageValue: string | null) =>
  textEnumFromStorageValue(storageValue, numbeNotationEnumMap)

export const useFormatNumberNotation = () =>
  useLocalStorageValue<NumberNotation>(
    FORMAT_NUMBER_NOTATION_KEY,
    DEFAULT_NUMBER_NOTATION,
    numberNotationFromStorageValue,
    textEnumToStorageValue,
  )
