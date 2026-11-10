import { formatEventAge } from "./eventAge"

describe("formatEventAge", () => {
  const now = new Date("2026-11-03T19:00:00Z")

  it.each([
    ["2026-11-03T18:59:34Z", "26s"],
    ["2026-11-03T18:55:00Z", "5m"],
    ["2026-11-03T18:42:10Z", "17m"],
    ["2026-11-03T16:30:00Z", "2h30m"],
    ["2026-11-03T16:00:00Z", "3h"],
    ["2026-11-01T19:00:00Z", "2d"],
  ])("formats %s as %s", (timestamp, expected) => {
    expect(formatEventAge(timestamp, now)).toBe(expected)
  })

  it("clamps timestamps in the future to 0s", () => {
    expect(formatEventAge("2026-11-03T19:00:05Z", now)).toBe("0s")
  })

  it("returns an empty string for a missing or invalid timestamp", () => {
    expect(formatEventAge(undefined, now)).toBe("")
    expect(formatEventAge("not a date", now)).toBe("")
  })
})
