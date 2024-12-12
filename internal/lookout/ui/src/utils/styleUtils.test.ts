import { getContrastText } from "./styleUtils"

describe("getContrastText", () => {
  it("returns black when no background color is provided", () => {
    expect(getContrastText("")).toEqual("#000")
  })

  it("returns black for a white background", () => {
    expect(getContrastText("#fff")).toEqual("#000")
  })

  it("returns white for a black background", () => {
    expect(getContrastText("#000")).toEqual("#fff")
  })

  it("returns white for a dark blue background", () => {
    expect(getContrastText("#000013")).toEqual("#fff")
  })

  it("returns black for a yellow background", () => {
    expect(getContrastText("#ffff00")).toEqual("#000")
  })
})
