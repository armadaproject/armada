import { JobRunDebugMessageFromJSON } from "../../../openapi/lookout"

import { fakeRunDebugMessage } from "./fakeData"

describe("fakeRunDebugMessage", () => {
  it("includes structured debug info, so that fake data mode shows the events table", () => {
    expect(fakeRunDebugMessage.debugInfo?.pod.events?.length).toBeGreaterThan(0)
  })

  it("has raw text that matches its structured debug info, as the API's responses do", () => {
    const reparsed = JobRunDebugMessageFromJSON({
      errorString: "",
      debugInfo: JSON.parse(fakeRunDebugMessage.errorString ?? ""),
    })

    expect(reparsed.debugInfo).toEqual(fakeRunDebugMessage.debugInfo)
  })
})
