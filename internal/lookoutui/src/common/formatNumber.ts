import { BROWSER_LOCALE, getBrowserSupportedLocale, SupportedLocale } from "./locales"

export const NUMBER_NOTATIONS = ["standard", "compact", "scientific", "engineering"] as const
export const numberNotationsSet = new Set<string>(NUMBER_NOTATIONS)

export type NumberNotation = (typeof NUMBER_NOTATIONS)[number]

export const DEFAULT_NUMBER_NOTATION: NumberNotation = "standard"

export const numberNotationDisplayNames: Record<NumberNotation, string> = {
  standard: "Standard",
  compact: "Compact",
  scientific: "Scientific",
  engineering: "Engineering",
}

export const isNumberNotation = (n: string): n is NumberNotation => numberNotationsSet.has(n)

export interface FormatNumberOptions {
  locale: SupportedLocale | typeof BROWSER_LOCALE
  notation: NumberNotation
}

export const formatNumber = (n: number, { locale, notation }: FormatNumberOptions) =>
  Intl.NumberFormat(locale === BROWSER_LOCALE ? getBrowserSupportedLocale() : locale, { notation }).format(n)
