import { JobState } from "../models/lookoutModels"

import { formatJobState } from "./jobsTableFormatters"

describe("JobsTableFormatters", () => {
  beforeEach(() => {
    vi.useFakeTimers().setSystemTime(new Date("2022-12-13T13:00:00.000Z"))
  })

  afterEach(() => {
    vi.runOnlyPendingTimers()
    vi.useRealTimers()
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
})
