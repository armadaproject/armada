import { createMemoryHistory, History } from "history"
import { ColumnId, createAnnotationColumn, JOB_COLUMNS } from "utils/jobsTableColumns"

import {
  BLANK_PREFERENCES,
  DEFAULT_PREFERENCES,
  JobsTablePreferences,
  JobsTablePreferencesService,
} from "./JobsTablePreferencesService"

describe("JobsTablePreferencesService", () => {
  let history: History, service: JobsTablePreferencesService

  beforeEach(() => {
    history = createMemoryHistory()
    service = new JobsTablePreferencesService(history)
  })

  describe("getInitialUserPrefs", () => {
    it("gives default preferences if no query params", () => {
      expect(service.getUserPrefs()).toStrictEqual(DEFAULT_PREFERENCES)
    })

    it("merges blank config with provided query params", () => {
      history.push({
        search: `?page=3&g[0]=state&sort[0][id]=jobId&sort[0][desc]=false`,
      })

      expect(service.getUserPrefs()).toMatchObject<Partial<JobsTablePreferences>>({
        ...BLANK_PREFERENCES,
        // From query string above
        pageIndex: 3,
        groupedColumns: ["state" as ColumnId],
        sortingState: [{ id: "jobId", desc: false }],
      })
    })
  })

  describe("saveNewPrefs", () => {
    it("does not remove other unrelated query params", () => {
      history.push({
        search: "?debug&someOtherKey=test",
      })

      service.saveNewPrefs(BLANK_PREFERENCES)

      expect(history.location.search).toContain("debug")
      expect(history.location.search).toContain("someOtherKey=test")
    })
  })

  describe("Page index", () => {
    it("round-trips 0", () => {
      savePrefWithDefaults({ pageIndex: 0 })
      expect(history.location.search).toContain("page=0")
      expect(service.getUserPrefs().pageIndex).toStrictEqual(0)
    })

    it("round-trips non-zero", () => {
      savePrefWithDefaults({ pageIndex: 5 })
      expect(history.location.search).toContain("page=5")
      expect(service.getUserPrefs().pageIndex).toStrictEqual(5)
    })
  })

  describe("Grouped columns", () => {
    it("round-trips columns", () => {
      savePrefWithDefaults({ groupedColumns: ["queue", "state"] as ColumnId[] })
      expect(history.location.search).toContain("g[0]=queue&g[1]=state")
      expect(service.getUserPrefs().groupedColumns).toStrictEqual(["queue", "state"])
    })

    it("round-trips empty list", () => {
      savePrefWithDefaults({ groupedColumns: [] })
      // Since the default is non-empty, then we assert that it's still in the query params
      expect(history.location.search).toContain("g[0]")
      expect(service.getUserPrefs().groupedColumns).toStrictEqual([])
    })
  })

  describe("Column filters", () => {
    it("round-trips column filters", () => {
      savePrefWithDefaults({ filterState: [{ id: "queue", value: "test" }] })
      expect(history.location.search).toContain("f[0][id]=queue&f[0][value]=test")
      expect(service.getUserPrefs().filterState).toStrictEqual([{ id: "queue", value: "test" }])
    })

    it("round-trips special characters", () => {
      savePrefWithDefaults({ filterState: [{ id: "queue", value: "test & why / do $ this" }] })
      expect(history.location.search).toContain("f[0][id]=queue&f[0][value]=test%20%26%20why%20%2F%20do%20%24%20this")
      expect(service.getUserPrefs().filterState).toStrictEqual([{ id: "queue", value: "test & why / do $ this" }])
    })

    it("round-trips empty list", () => {
      savePrefWithDefaults({ filterState: [] })
      expect(service.getUserPrefs().filterState).toStrictEqual([])
    })
  })

  describe("Sort order", () => {
    it("round-trips asc sort order", () => {
      savePrefWithDefaults({ sortingState: [{ id: "queue", desc: false }] })
      expect(history.location.search).toContain("sort[0][id]=queue&sort[0][desc]=false")
      expect(service.getUserPrefs().sortingState).toStrictEqual([{ id: "queue", desc: false }])
    })

    it("round-trips desc sort order", () => {
      savePrefWithDefaults({ sortingState: [{ id: "queue", desc: true }] })
      expect(history.location.search).toContain("sort[0][id]=queue&sort[0][desc]=true")
      expect(service.getUserPrefs().sortingState).toStrictEqual([{ id: "queue", desc: true }])
    })
  })

  describe("Column visibility", () => {
    it("round-trips visible columns", () => {
      savePrefWithDefaults({ visibleColumns: { queue: true, jobSet: false } })
      expect(history.location.search).toContain("vCols[0]=queue")
      expect(service.getUserPrefs().visibleColumns).toMatchObject({ queue: true, jobSet: false })
    })

    it("includes annotation columns", () => {
      savePrefWithDefaults({
        visibleColumns: { queue: true, jobSet: false, annotation_test: true, annotation_otherTest: false },
        allColumnsInfo: [...JOB_COLUMNS, createAnnotationColumn("test"), createAnnotationColumn("otherTest")],
      })
      expect(history.location.search).toContain("vCols[0]=queue&vCols[1]=annotation_test")
      expect(service.getUserPrefs().visibleColumns).toMatchObject({
        queue: true,
        jobSet: false,
        annotation_test: true,
        annotation_otherTest: false,
      })
    })
  })

  describe("Annotation columns", () => {
    it("round-trips user-added columns", () => {
      savePrefWithDefaults({ allColumnsInfo: [...JOB_COLUMNS, createAnnotationColumn("myAnnotation")] })
      expect(history.location.search).toContain("aCols[0]=myAnnotation")
      const cols = service.getUserPrefs().allColumnsInfo
      expect(cols.filter(({ id }) => id === "annotation_myAnnotation").length).toStrictEqual(1)
    })
  })

  describe("Expanded rows", () => {
    it("round-trips expanded rows", () => {
      savePrefWithDefaults({ expandedState: { myRowId: true, jobSet: false } })
      expect(history.location.search).toContain("e[0]=myRowId")
      expect(service.getUserPrefs().expandedState).toMatchObject({ myRowId: true })
    })

    it("round-trips zero expanded rows", () => {
      savePrefWithDefaults({ expandedState: {} })
      expect(history.location.search).not.toContain("e[0]=")
      expect(service.getUserPrefs().expandedState).toMatchObject({})
    })
  })

  describe("Page size", () => {
    it("round-trips page size", () => {
      savePrefWithDefaults({ pageSize: 123 })
      expect(history.location.search).toContain("pS=123")
      expect(service.getUserPrefs().pageSize).toStrictEqual(123)
    })
  })

  describe("Sidebar Job ID", () => {
    it("round-trips selected job", () => {
      savePrefWithDefaults({ sidebarJobId: "myJobId123" })
      expect(history.location.search).toContain("sb=myJobId123")
      expect(service.getUserPrefs().sidebarJobId).toStrictEqual("myJobId123")
    })

    it("round-trips no selected job", () => {
      savePrefWithDefaults({ sidebarJobId: undefined })
      expect(history.location.search).not.toContain("sb=")
      expect(service.getUserPrefs().sidebarJobId).toStrictEqual(undefined)
    })
  })

  describe("Column Widths", () => {
    it("round-trips column widths", () => {
      const colWidths = {
        state: 123,
        jobCount: 456,
        gpu: 890,
        jobSet: 1024,
      }
      savePrefWithDefaults({ columnSizing: colWidths })
      expect(service.getUserPrefs().columnSizing).toStrictEqual(colWidths)
    })

    it("round trips no column widths changed", () => {
      savePrefWithDefaults({ columnSizing: {} })
      expect(service.getUserPrefs().columnSizing).toStrictEqual({})
    })
  })

  describe("Sidebar width", () => {
    it("round-trips chosen width", () => {
      savePrefWithDefaults({ sidebarWidth: 123 })
      expect(service.getUserPrefs().sidebarWidth).toStrictEqual(123)
    })

    it("round trips with no selected width", () => {
      savePrefWithDefaults({ sidebarWidth: undefined })
      expect(service.getUserPrefs().sidebarWidth).toStrictEqual(undefined)
    })
  })

  const savePrefWithDefaults = (prefsToSave: Partial<JobsTablePreferences>) => {
    service.saveNewPrefs({
      ...BLANK_PREFERENCES,
      ...prefsToSave,
    })
  }
})
