import { Location, NavigateFunction, Params } from "react-router-dom"
import { ColumnId, DEFAULT_COLUMN_ORDER, StandardColumnId } from "utils/jobsTableColumns"

import { Match } from "../../models/lookoutV2Models"
import { Router } from "../../utils"
import {
  DEFAULT_PREFERENCES,
  JobsTablePreferences,
  JobsTablePreferencesService,
  PREFERENCES_KEY,
  QueryStringPrefs,
  stringifyQueryParams,
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
        ...DEFAULT_PREFERENCES,
      })
    })

    it("fills in default query params if empty", () => {
      router.navigate({
        search: `?page=3&g[0]=state&sort[id]=jobId&sort[desc]=false`,
      })

      expect(service.getUserPrefs()).toMatchObject<Partial<JobsTablePreferences>>({
        ...DEFAULT_PREFERENCES,
        // From query string above
        pageIndex: 3,
        groupedColumns: ["state" as ColumnId],
        order: { id: "jobId", direction: "ASC" },
      })
    })
  })

  describe("saveNewPrefs", () => {
    it("does not remove other unrelated query params", () => {
      router.navigate({
        search: "?debug&someOtherKey=test",
      })

      service.saveNewPrefs(DEFAULT_PREFERENCES)

      expect(router.location.search).toContain("debug")
      expect(router.location.search).toContain("someOtherKey=test")
    })
  })

  describe("Page index", () => {
    it("round-trips 0", () => {
      savePartialPrefs({ pageIndex: 0 })
      expect(router.location.search).toContain("page=0")
      expect(service.getUserPrefs().pageIndex).toStrictEqual(0)
    })

    it("round-trips non-zero", () => {
      savePartialPrefs({ pageIndex: 5 })
      expect(router.location.search).toContain("page=5")
      expect(service.getUserPrefs().pageIndex).toStrictEqual(5)
    })
  })

  describe("Grouped columns", () => {
    it("round-trips columns", () => {
      savePartialPrefs({ groupedColumns: ["queue", "state"] as ColumnId[] })
      expect(router.location.search).toContain("g[0]=queue&g[1]=state")
      expect(service.getUserPrefs().groupedColumns).toStrictEqual(["queue", "state"])
    })

    it("round-trips empty list", () => {
      savePartialPrefs({ groupedColumns: [] })
      expect(router.location.search).not.toContain("g[0]")
      expect(service.getUserPrefs().groupedColumns).toStrictEqual([])
    })
  })

  describe("Column filters", () => {
    it("round-trips column filters", () => {
      savePartialPrefs({ filters: [{ id: "queue", value: "test" }] })
      expect(router.location.search).toContain("f[0][id]=queue&f[0][value]=test&f[0][match]=startsWith")
      expect(service.getUserPrefs().filters).toStrictEqual([{ id: "queue", value: "test" }])
    })

    it("round-trips state filter", () => {
      savePartialPrefs({ filters: [{ id: "state", value: ["QUEUED", "PENDING", "RUNNING"] }] })
      expect(router.location.search).toContain(
        "f[0][id]=state&f[0][value][0]=QUEUED&f[0][value][1]=PENDING&f[0][value][2]=RUNNING&f[0][match]=anyOf",
      )
      expect(service.getUserPrefs().filters).toStrictEqual([
        { id: "state", value: ["QUEUED", "PENDING", "RUNNING"], match: Match.AnyOf },
      ])
    })

    it("round-trips special characters", () => {
      savePartialPrefs({ filters: [{ id: "queue", value: "test & why / do $ this" }] })
      expect(router.location.search).toContain(
        "f[0][id]=queue&f[0][value]=test%20%26%20why%20%2F%20do%20%24%20this&f[0][match]=startsWith",
      )
      expect(service.getUserPrefs().filters).toStrictEqual([
        { id: "queue", value: "test & why / do $ this", match: Match.StartsWith },
      ])
    })

    it("round-trips empty list", () => {
      savePartialPrefs({ filters: [] })
      expect(service.getUserPrefs().filters).toStrictEqual([])
    })
  })

  describe("Sort order", () => {
    it("round-trips asc sort order", () => {
      savePartialPrefs({ order: { id: "queue", direction: "ASC" } })
      expect(router.location.search).toContain("sort[id]=queue&sort[desc]=false")
      expect(service.getUserPrefs().order).toStrictEqual({ id: "queue", direction: "ASC" })
    })

    it("round-trips desc sort order", () => {
      savePartialPrefs({ order: { id: "queue", direction: "DESC" } })
      expect(router.location.search).toContain("sort[id]=queue&sort[desc]=true")
      expect(service.getUserPrefs().order).toStrictEqual({ id: "queue", direction: "DESC" })
    })
  })

  describe("Column visibility", () => {
    it("round-trips visible columns", () => {
      savePartialPrefs({ visibleColumns: { queue: true, jobSet: false } })
      expect(service.getUserPrefs().visibleColumns).toMatchObject({ queue: true, jobSet: false })
    })

    it("includes annotation columns", () => {
      savePartialPrefs({
        visibleColumns: { queue: true, jobSet: false, annotation_test: true, annotation_otherTest: false },
        annotationColumnKeys: ["test", "otherTest"],
      })
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
      savePartialPrefs({ annotationColumnKeys: ["myAnnotation"] })
      const cols = service.getUserPrefs().annotationColumnKeys
      expect(cols.filter((col) => col === "myAnnotation").length).toStrictEqual(1)
    })
  })

  describe("Expanded rows", () => {
    it("round-trips expanded rows", () => {
      savePartialPrefs({ expandedState: { myRowId: true, jobSet: false } })
      expect(router.location.search).toContain("e[0]=myRowId")
      expect(service.getUserPrefs().expandedState).toMatchObject({ myRowId: true })
    })

    it("round-trips zero expanded rows", () => {
      savePartialPrefs({ expandedState: {} })
      expect(router.location.search).not.toContain("e[0]=")
      expect(service.getUserPrefs().expandedState).toMatchObject({})
    })
  })

  describe("Page size", () => {
    it("round-trips page size", () => {
      savePartialPrefs({ pageSize: 123 })
      expect(router.location.search).toContain("ps=123")
      expect(service.getUserPrefs().pageSize).toStrictEqual(123)
    })
  })

  describe("Sidebar Job ID", () => {
    it("round-trips selected job", () => {
      savePartialPrefs({ sidebarJobId: "myJobId123" })
      expect(router.location.search).toContain("sb=myJobId123")
      expect(service.getUserPrefs().sidebarJobId).toStrictEqual("myJobId123")
    })

    it("round-trips no selected job", () => {
      savePartialPrefs({ sidebarJobId: undefined })
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
      savePartialPrefs({ columnSizing: colWidths })
      expect(service.getUserPrefs().columnSizing).toStrictEqual(colWidths)
    })

    it("round trips no column widths changed", () => {
      savePartialPrefs({ columnSizing: {} })
      expect(service.getUserPrefs().columnSizing).toStrictEqual({})
    })
  })

  describe("Sidebar width", () => {
    it("round-trips chosen width", () => {
      savePartialPrefs({ sidebarWidth: 123 })
      expect(service.getUserPrefs().sidebarWidth).toStrictEqual(123)
    })

    it("round trips with no selected width", () => {
      savePartialPrefs({ sidebarWidth: undefined })
      // Default is 600
      expect(service.getUserPrefs().sidebarWidth).toStrictEqual(600)
    })
  })

  describe("Queue parameters and Local storage", () => {
    it("should override for all query params even if only one is defined", () => {
      const queryParams: Partial<QueryStringPrefs> = {
        sb: "112233",
      }
      const localStorageParams: JobsTablePreferences = {
        annotationColumnKeys: ["hello"],
        expandedState: { foo: true },
        filters: [{ id: "jobId", value: "112233" }],
        columnMatches: { jobId: Match.Exact },
        groupedColumns: ["queue" as ColumnId, "jobSet" as ColumnId],
        order: { id: "timeInState", direction: "ASC" },
        pageIndex: 5,
        pageSize: 20,
        sidebarJobId: "223344",
        visibleColumns: { foo: true, bar: true },
      }
      localStorage.setItem(PREFERENCES_KEY, JSON.stringify(localStorageParams))
      router.navigate({
        search: stringifyQueryParams(queryParams),
      })
      expect(service.getUserPrefs()).toMatchObject({
        annotationColumnKeys: ["hello"],
        expandedState: {},
        filters: [],
        groupedColumns: [],
        order: DEFAULT_COLUMN_ORDER,
        pageIndex: 0,
        pageSize: 50,
        sidebarJobId: "112233",
        visibleColumns: { foo: true, bar: true },
      })
    })

    it("should override for all query params with multiple being defined", () => {
      const queryParams: Partial<QueryStringPrefs> = {
        f: [
          {
            id: StandardColumnId.State,
            value: ["QUEUED", "PENDING", "RUNNING"],
            match: Match.AnyOf,
          },
        ],
        sort: { id: "timeInState", desc: "false" },
        g: ["jobSet"],
      }
      const localStorageParams: JobsTablePreferences = {
        annotationColumnKeys: ["key"],
        expandedState: { foo: true },
        filters: [{ id: "jobId", value: "112233" }],
        columnMatches: { jobId: Match.Exact },
        groupedColumns: ["queue" as ColumnId, "jobSet" as ColumnId],
        order: { id: "timeInState", direction: "ASC" },
        pageIndex: 5,
        pageSize: 20,
        sidebarJobId: "223344",
        visibleColumns: { foo: true, bar: true },
      }
      localStorage.setItem(PREFERENCES_KEY, JSON.stringify(localStorageParams))
      router.navigate({
        search: stringifyQueryParams(queryParams),
      })
      expect(service.getUserPrefs()).toMatchObject({
        annotationColumnKeys: ["key"],
        expandedState: {},
        filters: [
          {
            id: StandardColumnId.State,
            value: ["QUEUED", "PENDING", "RUNNING"],
            match: Match.AnyOf,
          },
        ],
        groupedColumns: ["jobSet"],
        order: { id: "timeInState", direction: "ASC" },
        pageIndex: 0,
        pageSize: 50,
        sidebarJobId: undefined,
        visibleColumns: { foo: true, bar: true },
      })
    })
  })

  const savePartialPrefs = (prefsToSave: Partial<JobsTablePreferences>) => {
    service.saveNewPrefs({
      ...DEFAULT_PREFERENCES,
      ...prefsToSave,
    })
  }
})
