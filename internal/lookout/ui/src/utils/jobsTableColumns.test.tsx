import { formatSeconds } from "./jobsTableColumns"

describe("formatSeconds", () => {
  it("should return an empty string when seconds is undefined", () => {
    const result = formatSeconds(undefined)
    expect(result).toBe("")
  })

  it("should return an empty string when seconds is 0", () => {
    const result = formatSeconds(0)
    expect(result).toBe("")
  })

  it("should format seconds correctly when less than 60", () => {
    const result = formatSeconds(45)
    expect(result).toBe("45s")
  })

  it("should format minutes and seconds correctly when less than 3600", () => {
    const result = formatSeconds(135)
    expect(result).toBe("2m 15s")
  })

  it("should format hours, minutes, and seconds correctly when greater than 3600", () => {
    const result = formatSeconds(7265)
    expect(result).toBe("2h 1m 5s")
  })
})
