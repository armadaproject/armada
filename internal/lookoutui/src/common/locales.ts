// Identifiers for the set of common locales from which Lookout users can select for formatting options.
// See https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Intl#locales_argument.

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
  "pt-BR": "Portugese (Brazil)",
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
