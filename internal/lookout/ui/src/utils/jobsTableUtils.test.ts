import { diffOfKeys } from "./jobsTableUtils"

describe("JobsTableUtils", () => {
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
