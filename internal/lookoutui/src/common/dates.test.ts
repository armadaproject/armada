import { describe, expect, it } from "vitest"

import { validDateFromNullableIsoString } from "./dates"

describe("validDateFromNullableIsoString", () => {
  it("returns null when input is null", () => {
    expect(validDateFromNullableIsoString(null)).toBeNull()
  })

  it("returns null when input is an empty string", () => {
    expect(validDateFromNullableIsoString("")).toBeNull()
  })

  it("returns null when input is an invalid date string", () => {
    expect(validDateFromNullableIsoString("not-a-date")).toBeNull()
    expect(validDateFromNullableIsoString("invalid")).toBeNull()
    expect(validDateFromNullableIsoString("foo bar")).toBeNull()
  })

  it("returns a Date object when input is a valid ISO string", () => {
    const validDate = validDateFromNullableIsoString("2023-01-01T12:00:00Z")
    expect(validDate).toBeInstanceOf(Date)
    expect(validDate?.toISOString()).toBe("2023-01-01T12:00:00.000Z")
  })

  it("handles ISO strings with milliseconds correctly", () => {
    const validDate = validDateFromNullableIsoString("2023-01-01T12:00:00.123Z")
    expect(validDate).toBeInstanceOf(Date)
    expect(validDate?.toISOString()).toBe("2023-01-01T12:00:00.123Z")
  })

  it("handles ISO strings with timezone offsets correctly", () => {
    const validDate = validDateFromNullableIsoString("2023-01-01T12:00:00+02:00")
    expect(validDate).toBeInstanceOf(Date)
    // When converted to UTC, 12:00+02:00 becomes 10:00Z
    expect(validDate?.toISOString()).toBe("2023-01-01T10:00:00.000Z")
  })
})
