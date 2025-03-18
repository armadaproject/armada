import { afterEach, describe, expect, it, vi } from "vitest"

import { formatNumber, NUMBER_NOTATIONS, NumberNotation } from "./formatNumber"
import { SUPPORTED_LOCALES, SupportedLocale } from "./locales"

describe("formatNumber", () => {
  describe("using browser locale en-IN", () => {
    beforeEach(() => {
      vi.stubGlobal("navigator", { language: "en-IN" })
    })

    afterEach(() => {
      vi.unstubAllGlobals()
    })

    const testCasesLocaleEnIn: [number, Record<NumberNotation, string>][] = [
      [-10_000, { standard: "-10,000", compact: "-10K", scientific: "-1E4", engineering: "-10E3" }],
      [0, { standard: "0", compact: "0", scientific: "0E0", engineering: "0E0" }],
      [0.034, { standard: "0.034", compact: "0.034", scientific: "3.4E-2", engineering: "34E-3" }],
      [4, { standard: "4", compact: "4", scientific: "4E0", engineering: "4E0" }],
      [5.789, { standard: "5.789", compact: "5.8", scientific: "5.789E0", engineering: "5.789E0" }],
      [180, { standard: "180", compact: "180", scientific: "1.8E2", engineering: "180E0" }],
      [1_337, { standard: "1,337", compact: "1.3K", scientific: "1.337E3", engineering: "1.337E3" }],
      [42_866.29, { standard: "42,866.29", compact: "43K", scientific: "4.287E4", engineering: "42.866E3" }],
      [
        77_502_040_708,
        { standard: "77,50,20,40,708", compact: "7.8KCr", scientific: "7.75E10", engineering: "77.502E9" },
      ],
    ]

    testCasesLocaleEnIn.forEach(([n, expectedForNotation]) => {
      NUMBER_NOTATIONS.forEach((notation) => {
        it(`formats ${n} in ${notation} notation`, () => {
          expect(formatNumber(n, { locale: "browser", notation })).eq(expectedForNotation[notation])
        })
      })
    })
  })

  describe("using locale de", () => {
    const testCasesLocaleDe: [number, Record<NumberNotation, string>][] = [
      [-10_000, { standard: "-10.000", compact: "-10.000", scientific: "-1E4", engineering: "-10E3" }],
      [0, { standard: "0", compact: "0", scientific: "0E0", engineering: "0E0" }],
      [0.034, { standard: "0,034", compact: "0,034", scientific: "3,4E-2", engineering: "34E-3" }],
      [4, { standard: "4", compact: "4", scientific: "4E0", engineering: "4E0" }],
      [5.789, { standard: "5,789", compact: "5,8", scientific: "5,789E0", engineering: "5,789E0" }],
      [180, { standard: "180", compact: "180", scientific: "1,8E2", engineering: "180E0" }],
      [1_337, { standard: "1.337", compact: "1337", scientific: "1,337E3", engineering: "1,337E3" }],
      [42_866.29, { standard: "42.866,29", compact: "42.866", scientific: "4,287E4", engineering: "42,866E3" }],
      [
        77_502_040_708,
        { standard: "77.502.040.708", compact: "78\u00A0Mrd.", scientific: "7,75E10", engineering: "77,502E9" },
      ],
    ]

    testCasesLocaleDe.forEach(([n, expectedForNotation]) => {
      NUMBER_NOTATIONS.forEach((notation) => {
        it(`formats ${n} in ${notation} notation`, () => {
          expect(formatNumber(n, { locale: "de", notation })).eq(expectedForNotation[notation])
        })
      })
    })
  })

  const testCasesStandardNotation: Record<SupportedLocale, string> = {
    "en-IN": "49,72,92,23,812",
    "en-US": "49,729,223,812",
    "en-GB": "49,729,223,812",
    "en-AU": "49,729,223,812",
    fr: "49\u202F729\u202F223\u202F812",
    de: "49.729.223.812",
    es: "49.729.223.812",
    "es-MX": "49,729,223,812",
    "pt-BR": "49.729.223.812",
    it: "49.729.223.812",
    "zh-CN": "49,729,223,812",
    ru: "49\u00A0729\u00A0223\u00A0812",
    ja: "49,729,223,812",
    ar: "49,729,223,812",
  }

  SUPPORTED_LOCALES.forEach((locale) => {
    it(`formats 49,729,223,812 in standard notation for locale ${locale}`, () => {
      expect(formatNumber(49_729_223_812, { locale, notation: "standard" })).eq(testCasesStandardNotation[locale])
    })
  })
})
