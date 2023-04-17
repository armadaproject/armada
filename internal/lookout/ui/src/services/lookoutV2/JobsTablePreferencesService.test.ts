import { Location, NavigateFunction, Params } from "react-router-dom"
import { ColumnId, createAnnotationColumn, JOB_COLUMNS } from "utils/jobsTableColumns"

import { Router } from "../../utils"
import {
  BLANK_PREFERENCES,
  DEFAULT_LOCAL_STORAGE_PREFERENCES,
  DEFAULT_QUERY_PARAM_PREFERENCES,
  JobsTablePreferences,
  JobsTablePreferencesService,
} from "./JobsTablePreferencesService"

class FakeRouter implements Router {
  location: Location

  navigate: NavigateFunction = (to) => {
    if (typeof to === "number") {
      return
    }
    if (typeof to === "string") {
      this.location.pathname = to
      return
    }
    this.location.pathname = to.pathname ?? ""
    this.location.search = to.search ?? ""
  }
  params: Readonly<Params> = {}

  constructor() {
    this.location = { hash: "", key: "", pathname: "", search: "", state: undefined }
  }
}

describe("JobsTablePreferencesService", () => {
  let service: JobsTablePreferencesService
  let router: Router

  beforeEach(() => {
    router = new FakeRouter()
    service = new JobsTablePreferencesService(router)
  })

  describe("getInitialUserPrefs", () => {
    it("gives default preferences if no query params", () => {
      expect(service.getUserPrefs()).toStrictEqual({
        ...BLANK_PREFERENCES,
        ...DEFAULT_QUERY_PARAM_PREFERENCES,
        ...DEFAULT_LOCAL_STORAGE_PREFERENCES,
      })
    })

    it("merges blank config with provided query params", () => {
      router.navigate({
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
      router.navigate({
        search: "?debug&someOtherKey=test",
      })

      service.saveNewPrefs(BLANK_PREFERENCES)

      expect(router.location.search).toContain("debug")
      expect(router.location.search).toContain("someOtherKey=test")
    })
  })

  describe("Page index", () => {
    it("round-trips 0", () => {
      savePrefWithDefaults({ pageIndex: 0 })
      expect(router.location.search).toContain("page=0")
      expect(service.getUserPrefs().pageIndex).toStrictEqual(0)
    })

    it("round-trips non-zero", () => {
      savePrefWithDefaults({ pageIndex: 5 })
      expect(router.location.search).toContain("page=5")
      expect(service.getUserPrefs().pageIndex).toStrictEqual(5)
    })
  })

  describe("Grouped columns", () => {
    it("round-trips columns", () => {
      savePrefWithDefaults({ groupedColumns: ["queue", "state"] as ColumnId[] })
      expect(router.location.search).toContain("g[0]=queue&g[1]=state")
      expect(service.getUserPrefs().groupedColumns).toStrictEqual(["queue", "state"])
    })

    it("round-trips empty list", () => {
      savePrefWithDefaults({ groupedColumns: [] })
      // Since the default is non-empty, then we assert that it's still in the query params
      expect(router.location.search).toContain("g[0]")
      expect(service.getUserPrefs().groupedColumns).toStrictEqual([])
    })
  })

  describe("Column filters", () => {
    it("round-trips column filters", () => {
      savePrefWithDefaults({ filterState: [{ id: "queue", value: "test" }] })
      expect(router.location.search).toContain("f[0][id]=queue&f[0][value]=test")
      expect(service.getUserPrefs().filterState).toStrictEqual([{ id: "queue", value: "test" }])
    })

    it("round-trips special characters", () => {
      savePrefWithDefaults({ filterState: [{ id: "queue", value: "test & why / do $ this" }] })
      expect(router.location.search).toContain("f[0][id]=queue&f[0][value]=test%20%26%20why%20%2F%20do%20%24%20this")
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
      expect(router.location.search).toContain("sort[0][id]=queue&sort[0][desc]=false")
      expect(service.getUserPrefs().sortingState).toStrictEqual([{ id: "queue", desc: false }])
    })

    it("round-trips desc sort order", () => {
      savePrefWithDefaults({ sortingState: [{ id: "queue", desc: true }] })
      expect(router.location.search).toContain("sort[0][id]=queue&sort[0][desc]=true")
      expect(service.getUserPrefs().sortingState).toStrictEqual([{ id: "queue", desc: true }])
    })
  })

  describe("Column visibility", () => {
    it("round-trips visible columns", () => {
      savePrefWithDefaults({ visibleColumns: { queue: true, jobSet: false } })
      expect(router.location.search).toContain("vCols[0]=queue")
      expect(service.getUserPrefs().visibleColumns).toMatchObject({ queue: true, jobSet: false })
    })

    it("includes annotation columns", () => {
      savePrefWithDefaults({
        visibleColumns: { queue: true, jobSet: false, annotation_test: true, annotation_otherTest: false },
        annotationColumnKeys: ["test", "otherTest"],
      })
      expect(router.location.search).toContain("vCols[0]=queue&vCols[1]=annotation_test")
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
      savePrefWithDefaults({ annotationColumnKeys: ["myAnnotation"] })
      expect(router.location.search).toContain("aCols[0]=myAnnotation")
      const cols = service.getUserPrefs().annotationColumnKeys
      expect(cols.filter((col) => col === "myAnnotation").length).toStrictEqual(1)
    })
  })

  describe("Expanded rows", () => {
    it("round-trips expanded rows", () => {
      savePrefWithDefaults({ expandedState: { myRowId: true, jobSet: false } })
      expect(router.location.search).toContain("e[0]=myRowId")
      expect(service.getUserPrefs().expandedState).toMatchObject({ myRowId: true })
    })

    it("round-trips zero expanded rows", () => {
      savePrefWithDefaults({ expandedState: {} })
      expect(router.location.search).not.toContain("e[0]=")
      expect(service.getUserPrefs().expandedState).toMatchObject({})
    })
  })

  describe("Page size", () => {
    it("round-trips page size", () => {
      savePrefWithDefaults({ pageSize: 123 })
      expect(router.location.search).toContain("pS=123")
      expect(service.getUserPrefs().pageSize).toStrictEqual(123)
    })
  })

  describe("Sidebar Job ID", () => {
    it("round-trips selected job", () => {
      savePrefWithDefaults({ sidebarJobId: "myJobId123" })
      expect(router.location.search).toContain("sb=myJobId123")
      expect(service.getUserPrefs().sidebarJobId).toStrictEqual("myJobId123")
    })

    it("round-trips no selected job", () => {
      savePrefWithDefaults({ sidebarJobId: undefined })
      expect(router.location.search).not.toContain("sb=")
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
