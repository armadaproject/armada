import dayjs from "dayjs"
import ar from "dayjs/locale/ar"
import de from "dayjs/locale/de"
import en from "dayjs/locale/en"
import enAu from "dayjs/locale/en-au"
import enGb from "dayjs/locale/en-gb"
import enIn from "dayjs/locale/en-in"
import es from "dayjs/locale/es"
import esMx from "dayjs/locale/es-mx"
import fr from "dayjs/locale/fr"
import it from "dayjs/locale/it"
import ja from "dayjs/locale/ja"
import ptBr from "dayjs/locale/pt-br"
import ru from "dayjs/locale/ru"
import zhCn from "dayjs/locale/zh-cn"
import advancedFormatPlugin from "dayjs/plugin/advancedFormat"
import durationPlugin from "dayjs/plugin/duration"
import localizedFormatPlugin from "dayjs/plugin/localizedFormat"
import relativeTimePlugin from "dayjs/plugin/relativeTime"
import timeZonePlugin from "dayjs/plugin/timezone"
import utcPlugin from "dayjs/plugin/utc"

import { BROWSER_LOCALE, getBrowserSupportedLocale, SupportedLocale } from "./locales"
import { getBrowserTimeZone, tzName } from "./timeZones"

// Ensure that Day.js locales have been loaded for all supported locales
const supportedLocaleToDayJsLocale: Record<SupportedLocale, { localeData: en.Locale; localeName: string }> = {
  "en-US": {
    localeData: en,
    localeName: "en",
  },
  "en-GB": {
    localeData: enGb,
    localeName: "en-gb",
  },
  "en-IN": {
    localeData: enIn,
    localeName: "en-in",
  },
  "en-AU": {
    localeData: enAu,
    localeName: "en-au",
  },
  fr: {
    localeData: fr,
    localeName: "fr",
  },
  de: {
    localeData: de,
    localeName: "de",
  },
  es: {
    localeData: es,
    localeName: "es",
  },
  "es-MX": {
    localeData: esMx,
    localeName: "es-mx",
  },
  "pt-BR": {
    localeData: ptBr,
    localeName: "pt-br",
  },
  it: {
    localeData: it,
    localeName: "it",
  },
  "zh-CN": {
    localeData: zhCn,
    localeName: "zh-cn",
  },
  ru: {
    localeData: ru,
    localeName: "ru",
  },
  ja: {
    localeData: ja,
    localeName: "ja",
  },
  ar: {
    localeData: ar,
    localeName: "ar",
  },
}

dayjs.extend(advancedFormatPlugin)
dayjs.extend(utcPlugin)
dayjs.extend(timeZonePlugin)
dayjs.extend(localizedFormatPlugin)
dayjs.extend(relativeTimePlugin)
dayjs.extend(durationPlugin)

export const TIMESTAMP_FORMATS = [
  "full",
  "compact", // when using the compact format, we should display the time zone to the user using tzName() elsewhere
  "date-only",
] as const

export type TimestampFormat = (typeof TIMESTAMP_FORMATS)[number]

export const timestampFormatDisplayNames: Record<TimestampFormat, string> = {
  full: "Full timestamp",
  compact: "Compact timestamp",
  "date-only": "Date only",
}

const formatToDayJsTemplate: Record<TimestampFormat, string> = {
  full: "ll LTS",
  compact: "L LTS",
  "date-only": "ll",
}

export interface FormatTimestampOptions {
  locale: SupportedLocale | typeof BROWSER_LOCALE
  format: TimestampFormat
  timeZone: string
}

// If using this with user settings, consider using the useFormatIsoTimestampWithUserSettings() hook instead
export const formatTimestamp = (ts: Date | string, { locale, format, timeZone }: FormatTimestampOptions) => {
  const tsISOString = ts instanceof Date ? ts.toISOString() : ts
  const concreteTimeZone = timeZone || getBrowserTimeZone()
  const concreteSupportedLocale = locale === BROWSER_LOCALE ? getBrowserSupportedLocale() : locale

  let dayJsInstance = dayjs(tsISOString)

  try {
    dayJsInstance = dayJsInstance.locale(supportedLocaleToDayJsLocale[concreteSupportedLocale].localeName)
  } catch (e) {
    console.error(`Setting locale to '${concreteSupportedLocale}'`, e)
  }

  try {
    dayJsInstance = dayJsInstance.tz(concreteTimeZone)

    const nowUtcOffset = dayjs().tz(concreteTimeZone).utcOffset()
    const tsUtcOffset = dayJsInstance.utcOffset()

    dayJsInstance = dayJsInstance.subtract(tsUtcOffset - nowUtcOffset, "minute")
  } catch (e) {
    console.error(`Converting to time zone '${concreteTimeZone}'`, e)
  }

  const formattedValue = dayJsInstance.format(formatToDayJsTemplate[format])
  if (format === "full") {
    return `${formattedValue} ${tzName(concreteTimeZone)}`
  }
  return formattedValue
}

export const formatTimestampRelative = (ts: Date | string, humanize: boolean, withoutSuffix = false) => {
  const tsISOString = ts instanceof Date ? ts.toISOString() : ts

  const dayJsInstance = dayjs(tsISOString)

  if (humanize) {
    return dayJsInstance.fromNow(withoutSuffix)
  }

  return formatDuration(Math.abs(dayJsInstance.diff(dayjs(), "seconds")))
}

const durationFormatParts = ["Y[yr]", "M[mo]", "D[d]", "H[h]", "m[m]", "s[s]"]

export const formatDuration = (durationSeconds: number) => {
  const durationInstance = dayjs.duration(durationSeconds, "seconds")

  for (const [i, component] of [
    durationInstance.years(),
    durationInstance.months(),
    durationInstance.days(),
    durationInstance.hours(),
    durationInstance.minutes(),
    durationInstance.seconds(),
  ].entries()) {
    if (component !== 0) {
      return durationInstance.format(durationFormatParts.slice(i).join(" "))
    }
  }

  return "0s"
}
