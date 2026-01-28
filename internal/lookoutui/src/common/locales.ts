// Identifiers for the set of common locales from which Lookout users can select for formatting options.
// See https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Intl#locales_argument.

import dayJsLocaleAr from "dayjs/locale/ar"
import dayJsLocaleDe from "dayjs/locale/de"
import dayJsLocaleEn from "dayjs/locale/en"
import dayJsLocaleEnAu from "dayjs/locale/en-au"
import dayJsLocaleEnGb from "dayjs/locale/en-gb"
import dayJsLocaleEnIn from "dayjs/locale/en-in"
import dayJsLocaleEs from "dayjs/locale/es"
import dayJsLocaleEsMx from "dayjs/locale/es-mx"
import dayJsLocaleFr from "dayjs/locale/fr"
import dayJsLocaleIt from "dayjs/locale/it"
import dayJsLocaleJa from "dayjs/locale/ja"
import dayJsLocalePtBr from "dayjs/locale/pt-br"
import dayJsLocaleRu from "dayjs/locale/ru"
import dayJsLocaleZhCr from "dayjs/locale/zh-cn"

export const BROWSER_LOCALE = "browser" as const

export const SUPPORTED_LOCALES = [
  "en-US",
  "en-GB",
  "en-IN",
  "en-AU",
  "fr",
  "de",
  "es",
  "es-MX",
  "pt-BR",
  "it",
  "zh-CN",
  "ru",
  "ja",
  "ar",
] as const

export type SupportedLocale = (typeof SUPPORTED_LOCALES)[number]

// Ensures a dayjs locale is imported for each supported locale
export const dayJsLocales: Record<SupportedLocale, [string, ILocale]> = {
  "en-US": ["en", dayJsLocaleEn],
  "en-GB": ["en-gb", dayJsLocaleEnGb],
  "en-IN": ["en-in", dayJsLocaleEnIn],
  "en-AU": ["en-au", dayJsLocaleEnAu],
  fr: ["fr", dayJsLocaleFr],
  de: ["de", dayJsLocaleDe],
  es: ["es", dayJsLocaleEs],
  "es-MX": ["es-mx", dayJsLocaleEsMx],
  "pt-BR": ["pt-br", dayJsLocalePtBr],
  it: ["it", dayJsLocaleIt],
  "zh-CN": ["zh-cn", dayJsLocaleZhCr],
  ru: ["ru", dayJsLocaleRu],
  ja: ["ja", dayJsLocaleJa],
  ar: ["ar", dayJsLocaleAr],
}

const FALLBACK_LOCALE: SupportedLocale = "en-US"

export const supportedLocaleDisplayNames: Record<SupportedLocale, string> = {
  "en-US": "English (United States)",
  "en-GB": "English (United Kingdom)",
  "en-IN": "English (India)",
  "en-AU": "English (Australia)",
  fr: "French",
  de: "German",
  es: "Spanish",
  "es-MX": "Spanish (Mexico)",
  "pt-BR": "Portuguese (Brazil)",
  it: "Italian",
  "zh-CN": "Chinese (PRC)",
  ru: "Russian",
  ja: "Japanese",
  ar: "Arabic",
}

const supportedLocalesSet = new Set<string>(SUPPORTED_LOCALES)

export const isSupportedLocale = (l: string): l is SupportedLocale => supportedLocalesSet.has(l)

export const getBrowserSupportedLocale = (): SupportedLocale => {
  const browserLanguage = navigator.language
  for (const supportedLocale of SUPPORTED_LOCALES) {
    if (browserLanguage.toLowerCase() === supportedLocale.toLowerCase()) {
      return supportedLocale
    }
  }

  for (const supportedLocale of SUPPORTED_LOCALES) {
    if (browserLanguage.toLowerCase().split("-")[0] === supportedLocale.toLowerCase().split("-")[0]) {
      return supportedLocale
    }
  }

  return FALLBACK_LOCALE
}
