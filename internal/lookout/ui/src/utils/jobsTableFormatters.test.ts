import { JobState } from "models/lookoutV2Models"
import { formatBytes, formatCPU, formatJobState, formatTimeSince, formatUtcDate } from "./jobsTableFormatters"

jest
  .useFakeTimers()
  .setSystemTime(new Date('2022-12-13T13:00:00.000Z'));

describe("JobsTableFormatters", () => {
  describe("formatCPU", () => {
    it("formats CPU millis to CPU cores", () => {
      expect(formatCPU(2500)).toBe("2.5")
    })

    it("formats undefined to empty string", () => {
      expect(formatCPU(undefined)).toBe("")
    })
  })

  describe("formatJobState", () => {
    it("formats job states", () => {
      expect(formatJobState(JobState.Pending)).toBe("Pending")
    })

    it("ignores unknown job states", () => {
      expect(formatJobState("TESTING" as JobState)).toBe("TESTING")
    })

    it("formats undefined to empty string", () => {
      expect(formatJobState(undefined)).toBe("")
    })
  })

  describe("formatBytes", () => {
    it("formats bytes to human readable", () => {
      expect(formatBytes(2500)).toBe("2.5 kB")
    })

    it("formats undefined to empty string", () => {
      expect(formatBytes(undefined)).toBe("")
    })
  })

  describe("formatUtcDate", () => {
    it("formats dates to expected format", () => {
      expect(formatUtcDate("2022-12-12T12:19:14.956Z")).toBe("2022-12-12 12:19")
    })

    it("formats undefined to empty string", () => {
      expect(formatUtcDate(undefined)).toBe("")
    })
  })

  describe("formatTimeSince", () => {
    it("formats dates to expected format", () => {
      expect(formatTimeSince("2022-12-12T12:19:14.956Z")).toBe("1d 40m 45s")
    })

    it("formats undefined to empty string", () => {
      expect(formatTimeSince(undefined)).toBe("")
    })
  })
})
