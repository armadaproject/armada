import { fromRowId, GroupedRow, mergeSubRows, NonGroupedRow, RowId, toRowId } from "./reactTableUtils"

describe("ReactTableUtils", () => {
  describe("Row IDs", () => {
    const baseRowIdExample = "jobId:0"
    const childRowIdExample = "queue:queue-2>jobId:0"
    const deeplyNestedRowIdExample = "jobSet:job-set-2>queue:queue-2>jobId:0"

    describe("toRowId", () => {
      it("returns base row ID format", () => {
        const result = toRowId({
          type: "jobId",
          value: "0",
        })
        expect(result).toBe(baseRowIdExample)
      })

      it("returns child row ID format", () => {
        const result = toRowId({
          type: "jobId",
          value: "0",
          parentRowId: "queue:queue-2",
        })
        expect(result).toBe(childRowIdExample)
      })

      it("returns deeply nested row ID format", () => {
        const result = toRowId({
          type: "jobId",
          value: "0",
          parentRowId: "jobSet:job-set-2>queue:queue-2",
        })
        expect(result).toBe(deeplyNestedRowIdExample)
      })

      it("returns correct row ID with special characters", () => {
        const result = toRowId({
          type: "jobSet",
          value: "E:\\tmp\\file-share",
        })
        expect(result).toBe("jobSet:E%3A%5Ctmp%5Cfile-share")
      })
    })

    describe("fromRowId", () => {
      it("handles base row ID format", () => {
        const result = fromRowId(baseRowIdExample)
        expect(result).toStrictEqual({
          rowId: baseRowIdExample,
          rowIdPartsPath: [{ type: "jobId", value: "0" }],
          rowIdPathFromRoot: ["jobId:0"],
        })
      })

      it("handles child row ID format", () => {
        const result = fromRowId(childRowIdExample)
        expect(result).toStrictEqual({
          rowId: childRowIdExample,
          rowIdPartsPath: [
            { type: "queue", value: "queue-2" },
            { type: "jobId", value: "0" },
          ],
          rowIdPathFromRoot: ["queue:queue-2", "queue:queue-2>jobId:0"],
        })
      })

      it("handles deeply nested row ID format", () => {
        const result = fromRowId(deeplyNestedRowIdExample)
        expect(result).toStrictEqual({
          rowId: deeplyNestedRowIdExample,
          rowIdPartsPath: [
            { type: "jobSet", value: "job-set-2" },
            { type: "queue", value: "queue-2" },
            { type: "jobId", value: "0" },
          ],
          rowIdPathFromRoot: [
            "jobSet:job-set-2",
            "jobSet:job-set-2>queue:queue-2",
            "jobSet:job-set-2>queue:queue-2>jobId:0",
          ],
        })
      })

      it("handles special characters correctly", () => {
        const result = fromRowId("jobSet:E%3A%5Ctmp%5Cfile-share")
        expect(result).toStrictEqual({
          rowId: "jobSet:E%3A%5Ctmp%5Cfile-share",
          rowIdPartsPath: [
            {
              type: "jobSet",
              value: "E:\\tmp\\file-share",
            },
          ],
          rowIdPathFromRoot: ["jobSet:E%3A%5Ctmp%5Cfile-share"],
        })
      })
    })
  })

  describe("mergeSubRows", () => {
    let existingData: (NonGroupedRow | GroupedRow<any, any>)[],
      newRows: (NonGroupedRow | GroupedRow<any, any>)[],
      parentRowId: RowId | undefined,
      append = false

    it("returns given rows if no parent path given", () => {
      existingData = [{ rowId: "fruit:apple" }]
      newRows = [{ rowId: "fruit:banana" }]
      parentRowId = undefined

      const { rootData, parentRow } = mergeSubRows(existingData, newRows, parentRowId, append)

      expect(rootData).toStrictEqual(newRows)
      expect(parentRow).toBeUndefined()
    })

    it("merges in new rows at the correct location", () => {
      existingData = [
        { rowId: "fruit:apple", subRows: [] },
        { rowId: "fruit:banana", subRows: [] },
      ]
      newRows = [{ rowId: "taste:delicious" }]
      parentRowId = "fruit:banana"

      const { rootData, parentRow } = mergeSubRows(existingData, newRows, parentRowId, append)

      expect(rootData).toStrictEqual([
        { rowId: "fruit:apple", subRows: [] },
        { rowId: "fruit:banana", subRows: [{ rowId: "taste:delicious" }] },
      ])
      expect(parentRow).toStrictEqual({ rowId: "fruit:banana", subRows: [{ rowId: "taste:delicious" }] })
    })

    it("does not crash if merging failed", () => {
      existingData = [
        { rowId: "fruit:apple", subRows: [] },
        { rowId: "fruit:banana", subRows: [] },
      ]
      newRows = [{ rowId: "taste:delicious" }]
      parentRowId = "fruit:avocado"

      const { rootData, parentRow } = mergeSubRows(existingData, newRows, parentRowId, append)

      expect(rootData).toStrictEqual(existingData)
      expect(parentRow).toBeUndefined()
    })

    it("overrides existing subrows if not appending", () => {
      existingData = [{ rowId: "fruit:apple", subRows: [{ rowId: "color:green" }] }]
      newRows = [{ rowId: "taste:delicious" }]
      parentRowId = "fruit:apple"
      append = false

      const { rootData, parentRow } = mergeSubRows(existingData, newRows, parentRowId, append)

      expect(rootData).toStrictEqual([{ rowId: "fruit:apple", subRows: [{ rowId: "taste:delicious" }] }])
      expect(parentRow).toStrictEqual({ rowId: "fruit:apple", subRows: [{ rowId: "taste:delicious" }] })
    })

    it("appends existing subrows if appending", () => {
      existingData = [{ rowId: "fruit:apple", subRows: [{ rowId: "color:green" }] }]
      newRows = [{ rowId: "taste:delicious" }]
      parentRowId = "fruit:apple"
      append = true

      const { rootData, parentRow } = mergeSubRows(existingData, newRows, parentRowId, append)

      expect(rootData).toStrictEqual([
        { rowId: "fruit:apple", subRows: [{ rowId: "color:green" }, { rowId: "taste:delicious" }] },
      ])
      expect(parentRow).toStrictEqual({
        rowId: "fruit:apple",
        subRows: [{ rowId: "color:green" }, { rowId: "taste:delicious" }],
      })
    })
  })
})
