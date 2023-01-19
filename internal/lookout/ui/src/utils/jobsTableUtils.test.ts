import { convertRowPartsToFilters, diffOfKeys } from "./jobsTableUtils"

describe("JobsTableUtils", () => {
  describe("convertRowPartsToFilters", () => {
    it("returns empty if not expanding a row", () => {
      const result = convertRowPartsToFilters([])
      expect(result).toStrictEqual([])
    })

    it("returns one filter when expanding a top level row", () => {
      const result = convertRowPartsToFilters([{ type: "queue", value: "queue-2" }])
      expect(result).toStrictEqual([
        {
          field: "queue",
          value: "queue-2",
          match: "exact",
        },
      ])
    })

    it("returns multiple filters when expanding a nested row", () => {
      const result = convertRowPartsToFilters([
        { type: "jobSet", value: "job-set-2" },
        {
          type: "queue",
          value: "queue-2",
        },
      ])
      expect(result).toStrictEqual([
        {
          field: "jobSet",
          value: "job-set-2",
          match: "exact",
        },
        {
          field: "queue",
          value: "queue-2",
          match: "exact",
        },
      ])
    })
  })

  describe("diffOfKeys", () => {
    it("detects added keys", () => {
      const newObject = {
        hello: 5,
      }
      const previousObject = {}

      const [addedKeys, removedKeys] = diffOfKeys<string>(newObject, previousObject)

      expect(addedKeys).toStrictEqual(["hello"])
      expect(removedKeys).toStrictEqual([])
    })

    it("detects removed keys", () => {
      const newObject = {}
      const previousObject = {
        hello: 5,
      }

      const [addedKeys, removedKeys] = diffOfKeys<string>(newObject, previousObject)

      expect(addedKeys).toStrictEqual([])
      expect(removedKeys).toStrictEqual(["hello"])
    })

    it("handles multiple added and removed keys", () => {
      const newObject = {
        c: 101,
        d: 103,
        e: 104,
      }
      const previousObject = {
        a: 1,
        b: 2,
        c: 3,
      }

      const [addedKeys, removedKeys] = diffOfKeys<string>(newObject, previousObject)

      expect(addedKeys).toStrictEqual(["d", "e"])
      expect(removedKeys).toStrictEqual(["a", "b"])
    })
  })
})
