import { JobState } from "models/lookoutV2Models"

import { formatBytes, formatCPU, formatJobState, formatTimeSince, formatUtcDate } from "./jobsTableFormatters"

describe("JobsTableFormatters", () => {
  beforeEach(() => {
    jest.useFakeTimers().setSystemTime(new Date("2022-12-13T13:00:00.000Z"))
  })

  afterEach(() => {
    jest.runOnlyPendingTimers()
    jest.useRealTimers()
  })

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
      expect(formatBytes(2500)).toBe("2.44 kiB")
    })

    it("formats byte counts using binary numbers", () => {
      expect(formatBytes(5 * 1024 * 1024)).toBe("5 MiB")
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
      const now = new Date("2022-12-13T13:00:00.000Z").getTime()
      expect(formatTimeSince("2022-12-12T12:19:14.956Z", now)).toBe("1d 40m 45s")
    })

    it("formats date longer than a month", () => {
      const now = new Date("2023-01-25T16:27:00.000Z").getTime()
      expect(formatTimeSince("2022-12-12T12:19:14.956Z", now)).toBe("44d 4h 7m 45s")
    })

    it("formats undefined to empty string", () => {
      const now = new Date("2022-12-13T13:00:00.000Z").getTime()
      expect(formatTimeSince(undefined, now)).toBe("")
    })
  })
})
