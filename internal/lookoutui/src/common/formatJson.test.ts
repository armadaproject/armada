import { tryFormatJson } from "./formatJson"

describe("tryFormatJson", () => {
  it("indents JSON that is on one line", () => {
    expect(tryFormatJson('{"a":1,"b":[true,null]}')).toBe('{\n  "a": 1,\n  "b": [\n    true,\n    null\n  ]\n}')
  })

  it("keeps every field, including ones nothing else knows about", () => {
    expect(JSON.parse(tryFormatJson('{"known":1,"unknown":{"deep":[1,2]}}') ?? "")).toEqual({
      known: 1,
      unknown: { deep: [1, 2] },
    })
  })

  it("returns undefined for text that is not JSON", () => {
    expect(tryFormatJson("Events:\n  Type  Reason")).toBeUndefined()
    expect(tryFormatJson("")).toBeUndefined()
    expect(tryFormatJson('{"truncated": ')).toBeUndefined()
  })
})
