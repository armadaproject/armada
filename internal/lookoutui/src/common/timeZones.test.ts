import { afterEach, describe, expect, it, vi } from "vitest"

import { getTimeZoneOffsetAtTimestamp, tzName } from "./timeZones"

const northernHemisphereStandardTime = new Date("2020-01-01T00:00:00Z")
const northernHemisphereDaylightSavingTime = new Date("2020-07-01T00:00:00Z")

describe("getTimeZoneOffsetAtTimestamp", () => {
  it("returns Australia/Adelaide offset during southern-hemisphere standard time", () => {
    expect(getTimeZoneOffsetAtTimestamp("Australia/Adelaide", new Date("2020-06-02T21:07:09Z"))).toEqual({
      abbreviation: "ACST",
      offsetMinutes: -570,
    })
  })

  it("returns Australia/Adelaide offset during southern-hemisphere daylight saving time", () => {
    expect(getTimeZoneOffsetAtTimestamp("Australia/Adelaide", new Date("2019-01-02T21:07:09Z"))).toEqual({
      abbreviation: "ACDT",
      offsetMinutes: -630,
    })
  })

  it("returns America/Anchorage offset during northern-hemisphere standard time", () => {
    expect(getTimeZoneOffsetAtTimestamp("America/Anchorage", new Date("2021-12-25T06:00:43Z"))).toEqual({
      abbreviation: "AKST",
      offsetMinutes: 540,
    })
  })

  it("returns America/Anchorage offset during northern-hemisphere daylight saving time", () => {
    expect(getTimeZoneOffsetAtTimestamp("America/Anchorage", new Date("2023-07-01T13:02:34Z"))).toEqual({
      abbreviation: "AKDT",
      offsetMinutes: 480,
    })
  })

  it("returns Africa/Abidjan offset", () => {
    expect(getTimeZoneOffsetAtTimestamp("Africa/Abidjan", new Date("2023-07-01T13:02:34Z"))).toEqual({
      abbreviation: "GMT",
      offsetMinutes: 0,
    })
  })

  it("returns Asia/Urumqi offset", () => {
    expect(getTimeZoneOffsetAtTimestamp("Asia/Urumqi", new Date("2023-07-01T13:02:34Z"))).toEqual({
      abbreviation: "+06",
      offsetMinutes: -360,
    })
  })

  it("returns UTC offset for invalid tine zone indentifier", () => {
    expect(getTimeZoneOffsetAtTimestamp("i am not a valid tz identifier", new Date("2023-07-01T13:02:34Z"))).toEqual({
      abbreviation: "UTC",
      offsetMinutes: 0,
    })
  })
})

describe("tzName", () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it("returns Europe/Brussels time zone abbreviation when in standard time", () => {
    vi.setSystemTime(northernHemisphereStandardTime)
    expect(tzName("Europe/Brussels")).eq("CET")
  })

  it("returns Europe/Brussels time zone abbreviation when in daylight saving time", () => {
    vi.setSystemTime(northernHemisphereDaylightSavingTime)
    expect(tzName("Europe/Brussels")).eq("CEST")
  })

  it("returns Pacific/Auckland time zone abbreviation when in standard time", () => {
    vi.setSystemTime(northernHemisphereDaylightSavingTime)
    expect(tzName("Pacific/Auckland")).eq("NZST")
  })

  it("returns America/Punta_Arenas time zone abbreviation", () => {
    expect(tzName("America/Punta_Arenas")).eq("-03")
  })

  it("returns UTC abbreviation", () => {
    expect(tzName("Etc/UTC")).eq("UTC")
  })
})
