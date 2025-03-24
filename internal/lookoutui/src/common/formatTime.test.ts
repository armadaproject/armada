import { afterEach, describe, expect, it, vi } from "vitest"

import {
  formatDuration,
  formatTimestamp,
  formatTimestampRelative,
  TIMESTAMP_FORMATS,
  TimestampFormat,
} from "./formatTime"

const TEST_TIMESTAMP = "2024-04-12T12:22:26Z"

const northernHemisphereStandardTime = new Date("2020-01-01T00:00:00")
const northernHemisphereDaylightSavingTime = new Date("2020-07-01T00:00:00")

describe("formatTimestamp", () => {
  describe("using browser locale ja-JP and browser time zone Pacific/Chatham", () => {
    let originalBrowserTimeZone: string
    beforeAll(() => {
      originalBrowserTimeZone = Intl.DateTimeFormat().resolvedOptions().timeZone
    })

    beforeEach(() => {
      vi.stubGlobal("navigator", { language: "ja-JP" })

      const resolvedOptions = Intl.DateTimeFormat().resolvedOptions()
      Intl.DateTimeFormat.prototype.resolvedOptions = () => ({
        ...resolvedOptions,
        timeZone: "Pacific/Chatham",
      })

      vi.useFakeTimers()
    })

    afterEach(() => {
      vi.useRealTimers()

      const resolvedOptions = Intl.DateTimeFormat().resolvedOptions()
      Intl.DateTimeFormat.prototype.resolvedOptions = () => ({
        ...resolvedOptions,
        timeZone: originalBrowserTimeZone,
      })

      vi.unstubAllGlobals()
    })

    const testCasesLocaleJaJpTimeZonePacificChathamSDT: Record<TimestampFormat, string> = {
      full: "2024年4月13日 01:07:26 +1245",
      compact: "2024/04/13 01:07:26",
      "date-only": "2024年4月13日",
    }

    const testCasesLocaleJaJpTimeZonePacificChathamDST: Record<TimestampFormat, string> = {
      full: "2024年4月13日 02:07:26 +1345",
      compact: "2024/04/13 02:07:26",
      "date-only": "2024年4月13日",
    }

    TIMESTAMP_FORMATS.forEach((format) => {
      it(`formats in ${format} for standard time`, () => {
        vi.setSystemTime(northernHemisphereDaylightSavingTime.valueOf())
        expect(formatTimestamp(TEST_TIMESTAMP, { locale: "browser", format, timeZone: "" })).eq(
          testCasesLocaleJaJpTimeZonePacificChathamSDT[format],
        )
      })

      it(`formats in ${format} for daylight saving time`, () => {
        vi.setSystemTime(northernHemisphereStandardTime.valueOf())
        expect(formatTimestamp(TEST_TIMESTAMP, { locale: "browser", format, timeZone: "" })).eq(
          testCasesLocaleJaJpTimeZonePacificChathamDST[format],
        )
      })
    })
  })

  describe("using locale en-GB and time zone Europe/London", () => {
    beforeEach(() => {
      vi.useFakeTimers()
    })

    afterEach(() => {
      vi.useRealTimers()
    })

    const testCasesLocaleEnGbTimeZoneEuropeLondon: Record<TimestampFormat, [string, string]> = {
      full: ["12 Apr 2024 12:22:26 GMT", "12 Apr 2024 13:22:26 BST"],
      compact: ["12/04/2024 12:22:26", "12/04/2024 13:22:26"],
      "date-only": ["12 Apr 2024", "12 Apr 2024"],
    }

    const testCasesLocaleEnGbTimeZoneEuropeLondonSDT: Record<TimestampFormat, string> = {
      full: "12 Apr 2024 12:22:26 GMT",
      compact: "12/04/2024 12:22:26",
      "date-only": "12 Apr 2024",
    }

    const testCasesLocaleEnGbTimeZoneEuropeLondonDST: Record<TimestampFormat, string> = {
      full: "12 Apr 2024 13:22:26 BST",
      compact: "12/04/2024 13:22:26",
      "date-only": "12 Apr 2024",
    }

    TIMESTAMP_FORMATS.forEach((format) => {
      it(`formats in ${format} for standard time`, () => {
        vi.setSystemTime(northernHemisphereStandardTime)

        expect(formatTimestamp(TEST_TIMESTAMP, { locale: "en-GB", format, timeZone: "Europe/London" })).eq(
          testCasesLocaleEnGbTimeZoneEuropeLondonSDT[format],
        )
      })

      it(`formats in ${format} for daylight saving time`, () => {
        vi.setSystemTime(northernHemisphereDaylightSavingTime)

        expect(formatTimestamp(TEST_TIMESTAMP, { locale: "en-GB", format, timeZone: "Europe/London" })).eq(
          testCasesLocaleEnGbTimeZoneEuropeLondonDST[format],
        )
      })
    })

    // Since Europe/London observes daylight saving time, formatTimestamp() can return one of two options depending of the time of year
    TIMESTAMP_FORMATS.forEach((format) => {
      it(`formats in ${format}`, () => {
        expect(formatTimestamp(TEST_TIMESTAMP, { locale: "en-GB", format, timeZone: "Europe/London" })).toBeOneOf(
          testCasesLocaleEnGbTimeZoneEuropeLondon[format],
        )
      })
    })
  })

  describe("using locale en-US and time zone Etc/UTC", () => {
    const testCasesLocaleEnUsTimeZoneUTC: Record<TimestampFormat, string> = {
      full: "Apr 12, 2024 12:22:26 PM UTC",
      compact: "04/12/2024 12:22:26 PM",
      "date-only": "Apr 12, 2024",
    }

    TIMESTAMP_FORMATS.forEach((format) => {
      it(`formats in ${format}`, () => {
        expect(formatTimestamp(TEST_TIMESTAMP, { locale: "en-US", format, timeZone: "Etc/UTC" })).eq(
          testCasesLocaleEnUsTimeZoneUTC[format],
        )
      })
    })
  })
})

describe("formatTimestampRelative", () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  const twoMinutesThreeSecondsBeforeTestTimestamp = new Date("2024-04-12T12:20:23Z")

  it("formats for time two minutes and three seconds in the future without humanize, without suffix", () => {
    vi.setSystemTime(twoMinutesThreeSecondsBeforeTestTimestamp)
    expect(formatTimestampRelative(TEST_TIMESTAMP, false, true)).eq("2m 3s")
  })

  it("formats for time two minutes and three seconds in the future without humanize, with suffix", () => {
    vi.setSystemTime(twoMinutesThreeSecondsBeforeTestTimestamp)
    expect(formatTimestampRelative(TEST_TIMESTAMP, false, false)).eq("2m 3s")
  })

  it("formats for time two minutes and three seconds in the future with humanize, without suffix", () => {
    vi.setSystemTime(twoMinutesThreeSecondsBeforeTestTimestamp)
    expect(formatTimestampRelative(TEST_TIMESTAMP, true, true)).eq("2 minutes")
  })

  it("formats for time two minutes and three seconds in the future with humanize, with suffix", () => {
    vi.setSystemTime(twoMinutesThreeSecondsBeforeTestTimestamp)
    expect(formatTimestampRelative(TEST_TIMESTAMP, true, false)).eq("in 2 minutes")
  })

  const sevenMinutesFiftySixSecondsAfterTestTimestamp = new Date("2024-04-12T12:29:22Z")

  it("formats for time seven minutes and fifty-six seconds in the past without humanize, without suffix", () => {
    vi.setSystemTime(sevenMinutesFiftySixSecondsAfterTestTimestamp)
    expect(formatTimestampRelative(TEST_TIMESTAMP, false, true)).eq("6m 56s")
  })

  it("formats for time seven minutes and fifty-six seconds in the past without humanize, with suffix", () => {
    vi.setSystemTime(sevenMinutesFiftySixSecondsAfterTestTimestamp)
    expect(formatTimestampRelative(TEST_TIMESTAMP, false, false)).eq("6m 56s")
  })

  it("formats for time seven minutes and fifty-six seconds in the past with humanize, without suffix", () => {
    vi.setSystemTime(sevenMinutesFiftySixSecondsAfterTestTimestamp)
    expect(formatTimestampRelative(TEST_TIMESTAMP, true, true)).eq("7 minutes")
  })

  it("formats for time seven minutes and fifty-six seconds in the past with humanize, with suffix", () => {
    vi.setSystemTime(sevenMinutesFiftySixSecondsAfterTestTimestamp)
    expect(formatTimestampRelative(TEST_TIMESTAMP, true, false)).eq("7 minutes ago")
  })

  const oneHourSevenMinutesFiftySixSecondsAfterTestTimestamp = new Date("2024-04-12T13:29:22Z")

  it("formats for time one hour, seven minutes and fifty-six seconds in the past without humanize, without suffix", () => {
    vi.setSystemTime(oneHourSevenMinutesFiftySixSecondsAfterTestTimestamp)
    expect(formatTimestampRelative(TEST_TIMESTAMP, false, true)).eq("1h 6m 56s")
  })

  it("formats for time one hour, seven minutes and fifty-six seconds in the past without humanize, with suffix", () => {
    vi.setSystemTime(oneHourSevenMinutesFiftySixSecondsAfterTestTimestamp)
    expect(formatTimestampRelative(TEST_TIMESTAMP, false, false)).eq("1h 6m 56s")
  })

  it("formats for time one hour, seven minutes and fifty-six seconds in the past with humanize, without suffix", () => {
    vi.setSystemTime(oneHourSevenMinutesFiftySixSecondsAfterTestTimestamp)
    expect(formatTimestampRelative(TEST_TIMESTAMP, true, true)).eq("an hour")
  })

  it("formats for time one hour, seven minutes and fifty-six seconds in the past with humanize, with suffix", () => {
    vi.setSystemTime(oneHourSevenMinutesFiftySixSecondsAfterTestTimestamp)
    expect(formatTimestampRelative(TEST_TIMESTAMP, true, false)).eq("an hour ago")
  })

  const nineDaysTwentyTwoHoursFortyFiveMinutesNineteenSecondsAfterTestTimestamp = new Date("2024-04-22T11:07:45Z")

  it("formats for time nine days, twenty-two hours, forty-five minutes and nineteen seconds in the past without humanize, without suffix", () => {
    vi.setSystemTime(nineDaysTwentyTwoHoursFortyFiveMinutesNineteenSecondsAfterTestTimestamp)
    expect(formatTimestampRelative(TEST_TIMESTAMP, false, true)).eq("9d 22h 45m 19s")
  })

  it("formats for time nine days, twenty-two hours, forty-five minutes and nineteen seconds in the past without humanize, with suffix", () => {
    vi.setSystemTime(nineDaysTwentyTwoHoursFortyFiveMinutesNineteenSecondsAfterTestTimestamp)
    expect(formatTimestampRelative(TEST_TIMESTAMP, false, false)).eq("9d 22h 45m 19s")
  })

  it("formats for time nine days, twenty-two hours, forty-five minutes and nineteen seconds in the past with humanize, without suffix", () => {
    vi.setSystemTime(nineDaysTwentyTwoHoursFortyFiveMinutesNineteenSecondsAfterTestTimestamp)
    expect(formatTimestampRelative(TEST_TIMESTAMP, true, true)).eq("10 days")
  })

  it("formats for time nine days, twenty-two hours, forty-five minutes and nineteen seconds in the past with humanize, with suffix", () => {
    vi.setSystemTime(nineDaysTwentyTwoHoursFortyFiveMinutesNineteenSecondsAfterTestTimestamp)
    expect(formatTimestampRelative(TEST_TIMESTAMP, true, false)).eq("10 days ago")
  })
})

describe("formatDuration", () => {
  it("formats -300,617,443 seconds", () => {
    expect(formatDuration(-300_617_443)).eq("-9yr -6mo -11d -20h -50m -43s")
  })

  it("formats 3,617,443 seconds", () => {
    expect(formatDuration(3_617_443)).eq("1mo 11d 10h 50m 43s")
  })

  it("formats 2,628,001 seconds", () => {
    expect(formatDuration(2_628_001)).eq("1mo 0d 0h 0m 1s")
  })

  it("formats 2,628,000 seconds", () => {
    expect(formatDuration(2_628_000)).eq("1mo 0d 0h 0m 0s")
  })

  it("formats 1,083,998 seconds", () => {
    expect(formatDuration(1_083_998)).eq("12d 13h 6m 38s")
  })

  it("formats 10,333 seconds", () => {
    expect(formatDuration(10_333)).eq("2h 52m 13s")
  })

  it("formats 1,234 seconds", () => {
    expect(formatDuration(1_234)).eq("20m 34s")
  })

  it("formats 57 seconds", () => {
    expect(formatDuration(57)).eq("57s")
  })

  it("formats 3 seconds", () => {
    expect(formatDuration(3)).eq("3s")
  })

  it("formats 3.278564 seconds", () => {
    expect(formatDuration(3.278564)).eq("3s")
  })

  it("formats 0.278564 seconds", () => {
    expect(formatDuration(0.278564)).eq("0s")
  })

  it("formats 0 seconds", () => {
    expect(formatDuration(0)).eq("0s")
  })
})
