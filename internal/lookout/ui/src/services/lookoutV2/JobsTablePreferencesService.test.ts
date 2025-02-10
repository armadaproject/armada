import { Location, NavigateFunction, Params } from "react-router-dom"

import {
  DEFAULT_PREFERENCES,
  ensurePreferencesAreConsistent,
  JobsTablePreferences,
  JobsTablePreferencesService,
  PREFERENCES_KEY,
  QueryStringPrefs,
  stringifyQueryParams,
} from "./JobsTablePreferencesService"
import { Match } from "../../models/lookoutV2Models"
import { Router } from "../../utils"
import { ColumnId, DEFAULT_COLUMN_ORDERING, StandardColumnId } from "../../utils/jobsTableColumns"

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
        groupedColumns: [StandardColumnId.State],
        order: { id: StandardColumnId.JobID, direction: "ASC" },
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
      savePartialPrefs({ groupedColumns: [StandardColumnId.Queue, StandardColumnId.State] })
      expect(router.location.search).toContain("g[0]=queue&g[1]=state")
      expect(service.getUserPrefs().groupedColumns).toStrictEqual([StandardColumnId.Queue, StandardColumnId.State])
    })

    it("round-trips empty list", () => {
      savePartialPrefs({ groupedColumns: [] })
      expect(router.location.search).not.toContain("g[0]")
      expect(service.getUserPrefs().groupedColumns).toStrictEqual([])
    })

    it("creates annotation columns if grouping by annotation", () => {
      router.navigate({
        search: "?g[0]=annotation_my-annotation",
      })

      const prefs = service.getUserPrefs()
      expect(prefs.groupedColumns).toStrictEqual(["annotation_my-annotation"])
      expect(prefs.annotationColumnKeys).toStrictEqual(["my-annotation"])
    })

    it("ignores grouping overlapping groups specified", () => {
      router.navigate({
        search: "?g[0]=queue&g[0]=annotation_my-annotation",
      })

      const prefs = service.getUserPrefs()
      expect(prefs.groupedColumns).toStrictEqual([])
      expect(prefs.annotationColumnKeys).toStrictEqual([])
    })
  })

  describe("Column filters", () => {
    it("makes filter value and match types consistent (migrating to anyOf)", () => {
      savePartialPrefs({ filters: [{ id: StandardColumnId.Queue, value: "i-am-a-string-and-not-an-array" }] })
      expect(router.location.search).toContain(
        "f[0][id]=queue&f[0][value]=i-am-a-string-and-not-an-array&f[0][match]=anyOf",
      )
      expect(service.getUserPrefs().filters).toStrictEqual([{ id: StandardColumnId.Queue, value: undefined }])
    })

    it("makes filter value and match types consistent (migrating from anyOf)", () => {
      savePartialPrefs({ filters: [{ id: StandardColumnId.JobID, value: ["hello", "i", "am", "an", "array"] }] })
      expect(router.location.search).toContain(
        "f[0][id]=jobId&f[0][value][0]=hello&f[0][value][1]=i&f[0][value][2]=am&f[0][value][3]=an&f[0][value][4]=array&f[0][match]=exact",
      )
      expect(service.getUserPrefs().filters).toStrictEqual([{ id: StandardColumnId.JobID, value: "hello" }])
    })

    it("round-trips column filters", () => {
      savePartialPrefs({ filters: [{ id: StandardColumnId.Queue, value: ["test"] }] })
      expect(router.location.search).toContain("f[0][id]=queue&f[0][value][0]=test&f[0][match]=anyOf")
      expect(service.getUserPrefs().filters).toStrictEqual([{ id: StandardColumnId.Queue, value: ["test"] }])
    })

    it("round-trips state filter", () => {
      savePartialPrefs({ filters: [{ id: StandardColumnId.State, value: ["QUEUED", "PENDING", "RUNNING"] }] })
      expect(router.location.search).toContain(
        "f[0][id]=state&f[0][value][0]=QUEUED&f[0][value][1]=PENDING&f[0][value][2]=RUNNING&f[0][match]=anyOf",
      )
      expect(service.getUserPrefs().filters).toStrictEqual([
        { id: StandardColumnId.State, value: ["QUEUED", "PENDING", "RUNNING"] },
      ])
      expect(service.getUserPrefs().columnMatches[StandardColumnId.State]).toEqual(Match.AnyOf)
    })

    it("round-trips special characters", () => {
      savePartialPrefs({ filters: [{ id: StandardColumnId.Queue, value: ["test & why / do $ this"] }] })
      expect(router.location.search).toContain(
        "f[0][id]=queue&f[0][value][0]=test%20%26%20why%20%2F%20do%20%24%20this&f[0][match]=anyOf",
      )
      expect(service.getUserPrefs().filters).toStrictEqual([
        { id: StandardColumnId.Queue, value: ["test & why / do $ this"] },
      ])
      expect(service.getUserPrefs().columnMatches[StandardColumnId.Queue]).toEqual(Match.AnyOf)
    })

    it("round-trips empty list", () => {
      savePartialPrefs({ filters: [] })
      expect(service.getUserPrefs().filters).toStrictEqual([])
    })
  })

  describe("Sort order", () => {
    it("round-trips asc sort order", () => {
      savePartialPrefs({ order: { id: StandardColumnId.Queue, direction: "ASC" } })
      expect(router.location.search).toContain("sort[id]=queue&sort[desc]=false")
      expect(service.getUserPrefs().order).toStrictEqual({ id: StandardColumnId.Queue, direction: "ASC" })
    })

    it("round-trips desc sort order", () => {
      savePartialPrefs({ order: { id: StandardColumnId.Queue, direction: "DESC" } })
      expect(router.location.search).toContain("sort[id]=queue&sort[desc]=true")
      expect(service.getUserPrefs().order).toStrictEqual({ id: StandardColumnId.Queue, direction: "DESC" })
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
        filters: [{ id: StandardColumnId.JobID, value: "112233" }],
        columnMatches: { jobId: Match.Exact },
        groupedColumns: [StandardColumnId.Queue, StandardColumnId.JobSet],
        columnOrder: ["annotation_hello", StandardColumnId.JobID],
        order: { id: StandardColumnId.TimeInState, direction: "ASC" },
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
        columnOrder: [
          "annotation_hello",
          StandardColumnId.JobID,
          StandardColumnId.Queue,
          StandardColumnId.Namespace,
          StandardColumnId.JobSet,
          StandardColumnId.State,
          StandardColumnId.Count,
          StandardColumnId.Priority,
          StandardColumnId.Owner,
          StandardColumnId.CPU,
          StandardColumnId.Memory,
          StandardColumnId.EphemeralStorage,
          StandardColumnId.GPU,
          StandardColumnId.PriorityClass,
          StandardColumnId.LastTransitionTimeUtc,
          StandardColumnId.TimeInState,
          StandardColumnId.TimeSubmittedUtc,
          StandardColumnId.TimeSubmittedAgo,
          StandardColumnId.Node,
          StandardColumnId.Cluster,
          StandardColumnId.ExitCode,
          StandardColumnId.RuntimeSeconds,
        ],
        order: DEFAULT_COLUMN_ORDERING,
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
        sort: { id: StandardColumnId.TimeInState, desc: "false" },
        g: [StandardColumnId.JobSet],
      }
      const localStorageParams: JobsTablePreferences = {
        annotationColumnKeys: ["key"],
        expandedState: { foo: true },
        filters: [{ id: StandardColumnId.JobID, value: "112233" }],
        columnMatches: { jobId: Match.Exact },
        groupedColumns: [StandardColumnId.Queue, StandardColumnId.JobSet],
        columnOrder: [StandardColumnId.JobID, "annotation_key"],
        order: { id: StandardColumnId.TimeInState, direction: "ASC" },
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
          },
        ],
        columnMatches: { [StandardColumnId.State]: Match.AnyOf },
        groupedColumns: [StandardColumnId.JobSet],
        columnOrder: [
          StandardColumnId.JobID,
          "annotation_key",
          StandardColumnId.Queue,
          StandardColumnId.Namespace,
          StandardColumnId.JobSet,
          StandardColumnId.State,
          StandardColumnId.Count,
          StandardColumnId.Priority,
          StandardColumnId.Owner,
          StandardColumnId.CPU,
          StandardColumnId.Memory,
          StandardColumnId.EphemeralStorage,
          StandardColumnId.GPU,
          StandardColumnId.PriorityClass,
          StandardColumnId.LastTransitionTimeUtc,
          StandardColumnId.TimeInState,
          StandardColumnId.TimeSubmittedUtc,
          StandardColumnId.TimeSubmittedAgo,
          StandardColumnId.Node,
          StandardColumnId.Cluster,
          StandardColumnId.ExitCode,
          StandardColumnId.RuntimeSeconds,
        ],
        order: { id: StandardColumnId.TimeInState, direction: "ASC" },
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

describe("ensurePreferencesAreConsistent", () => {
  it("does not change valid preferences", () => {
    const validPreferences: JobsTablePreferences = {
      annotationColumnKeys: ["preferenc.es/foo-alpha", "preferenc.es/foo-bravo", "preferenc.es/foo-charlie"],
      expandedState: {},
      filters: [{ id: StandardColumnId.JobID, value: "112233" }],
      columnMatches: { jobId: Match.Exact },
      groupedColumns: [StandardColumnId.Queue, StandardColumnId.JobSet],
      columnOrder: [
        StandardColumnId.Owner,
        "annotation_preferenc.es/foo-bravo",
        StandardColumnId.JobID,
        StandardColumnId.State,
        "annotation_preferenc.es/foo-charlie",
        StandardColumnId.RuntimeSeconds,
        StandardColumnId.Queue,
        StandardColumnId.Namespace,
        StandardColumnId.JobSet,
        StandardColumnId.Priority,
        StandardColumnId.Count,
        StandardColumnId.Memory,
        StandardColumnId.EphemeralStorage,
        StandardColumnId.PriorityClass,
        StandardColumnId.TimeSubmittedUtc,
        StandardColumnId.CPU,
        StandardColumnId.ExitCode,
        StandardColumnId.LastTransitionTimeUtc,
        StandardColumnId.TimeInState,
        StandardColumnId.GPU,
        StandardColumnId.TimeSubmittedAgo,
        StandardColumnId.Cluster,
        "annotation_preferenc.es/foo-alpha",
        StandardColumnId.Node,
      ],
      order: { id: StandardColumnId.TimeInState, direction: "ASC" },
      pageIndex: 5,
      pageSize: 20,
      sidebarJobId: "223344",
      visibleColumns: {
        "annotation_preferenc.es/foo-charlie": true,
        "annotation_preferenc.es/foo-bravo": true,
        queue: true,
        jobId: true,
        jobSet: true,
        timeInState: true,
      },
    }

    ensurePreferencesAreConsistent(validPreferences)

    const expected: JobsTablePreferences = {
      annotationColumnKeys: ["preferenc.es/foo-alpha", "preferenc.es/foo-bravo", "preferenc.es/foo-charlie"],
      expandedState: {},
      filters: [{ id: StandardColumnId.JobID, value: "112233" }],
      columnMatches: { jobId: Match.Exact },
      groupedColumns: [StandardColumnId.Queue, StandardColumnId.JobSet],
      columnOrder: [
        StandardColumnId.Owner,
        "annotation_preferenc.es/foo-bravo",
        StandardColumnId.JobID,
        StandardColumnId.State,
        "annotation_preferenc.es/foo-charlie",
        StandardColumnId.RuntimeSeconds,
        StandardColumnId.Queue,
        StandardColumnId.Namespace,
        StandardColumnId.JobSet,
        StandardColumnId.Priority,
        StandardColumnId.Count,
        StandardColumnId.Memory,
        StandardColumnId.EphemeralStorage,
        StandardColumnId.PriorityClass,
        StandardColumnId.TimeSubmittedUtc,
        StandardColumnId.CPU,
        StandardColumnId.ExitCode,
        StandardColumnId.LastTransitionTimeUtc,
        StandardColumnId.TimeInState,
        StandardColumnId.GPU,
        StandardColumnId.TimeSubmittedAgo,
        StandardColumnId.Cluster,
        "annotation_preferenc.es/foo-alpha",
        StandardColumnId.Node,
      ],
      order: { id: StandardColumnId.TimeInState, direction: "ASC" },
      pageIndex: 5,
      pageSize: 20,
      sidebarJobId: "223344",
      visibleColumns: {
        "annotation_preferenc.es/foo-charlie": true,
        "annotation_preferenc.es/foo-bravo": true,
        queue: true,
        jobId: true,
        jobSet: true,
        timeInState: true,
      },
    }

    expect(validPreferences).toEqual(expected)
  })

  it("adds annotation key columns for annotations referenced in filters", () => {
    const validPreferences: JobsTablePreferences = {
      annotationColumnKeys: ["preferenc.es/foo-alpha"],
      expandedState: {},
      filters: [{ id: "annotation_preferenc.es/foo-delta", value: "ddd" }],
      columnMatches: { "annotation_preferenc.es/foo-delta": Match.StartsWith },
      groupedColumns: [],
      columnOrder: [
        StandardColumnId.JobID,
        StandardColumnId.Queue,
        StandardColumnId.Namespace,
        StandardColumnId.JobSet,
        StandardColumnId.State,
        StandardColumnId.Count,
        StandardColumnId.Priority,
        StandardColumnId.Owner,
        StandardColumnId.CPU,
        StandardColumnId.Memory,
        StandardColumnId.EphemeralStorage,
        StandardColumnId.GPU,
        StandardColumnId.PriorityClass,
        StandardColumnId.LastTransitionTimeUtc,
        StandardColumnId.TimeInState,
        StandardColumnId.TimeSubmittedUtc,
        StandardColumnId.TimeSubmittedAgo,
        StandardColumnId.Node,
        StandardColumnId.Cluster,
        StandardColumnId.ExitCode,
        StandardColumnId.RuntimeSeconds,
        "annotation_preferenc.es/foo-alpha",
      ],
      order: { id: StandardColumnId.TimeInState, direction: "ASC" },
      pageIndex: 5,
      pageSize: 20,
      sidebarJobId: "223344",
      visibleColumns: {
        "annotation_preferenc.es/foo-alpha": true,
        queue: true,
        jobId: true,
        jobSet: true,
        timeInState: true,
      },
    }

    ensurePreferencesAreConsistent(validPreferences)

    const expected: JobsTablePreferences = {
      annotationColumnKeys: [
        "preferenc.es/foo-alpha",
        "preferenc.es/foo-delta", // added
      ],
      expandedState: {},
      filters: [{ id: "annotation_preferenc.es/foo-delta", value: "ddd" }],
      columnMatches: { "annotation_preferenc.es/foo-delta": Match.StartsWith },
      groupedColumns: [],
      columnOrder: [
        StandardColumnId.JobID,
        StandardColumnId.Queue,
        StandardColumnId.Namespace,
        StandardColumnId.JobSet,
        StandardColumnId.State,
        StandardColumnId.Count,
        StandardColumnId.Priority,
        StandardColumnId.Owner,
        StandardColumnId.CPU,
        StandardColumnId.Memory,
        StandardColumnId.EphemeralStorage,
        StandardColumnId.GPU,
        StandardColumnId.PriorityClass,
        StandardColumnId.LastTransitionTimeUtc,
        StandardColumnId.TimeInState,
        StandardColumnId.TimeSubmittedUtc,
        StandardColumnId.TimeSubmittedAgo,
        StandardColumnId.Node,
        StandardColumnId.Cluster,
        StandardColumnId.ExitCode,
        StandardColumnId.RuntimeSeconds,
        "annotation_preferenc.es/foo-alpha",
        "annotation_preferenc.es/foo-delta", // added
      ],
      order: { id: StandardColumnId.TimeInState, direction: "ASC" },
      pageIndex: 5,
      pageSize: 20,
      sidebarJobId: "223344",
      visibleColumns: {
        "annotation_preferenc.es/foo-alpha": true,
        "annotation_preferenc.es/foo-delta": true, // added
        queue: true,
        jobId: true,
        jobSet: true,
        timeInState: true,
      },
    }

    expect(validPreferences).toEqual(expected)
  })

  it("makes grouped, ordered and filtered columns are visible", () => {
    const validPreferences: JobsTablePreferences = {
      annotationColumnKeys: ["preferenc.es/foo-alpha"],
      expandedState: {},
      filters: [
        { id: "annotation_preferenc.es/foo-alpha", value: "aaa" },
        {
          id: StandardColumnId.State,
          value: ["QUEUED", "PENDING", "RUNNING"],
        },
      ],
      columnMatches: { "annotation_preferenc.es/foo-alpha": Match.StartsWith, [StandardColumnId.State]: Match.AnyOf },
      groupedColumns: [StandardColumnId.Queue, "annotation_preferenc.es/foo-alpha", StandardColumnId.JobSet],
      columnOrder: [
        StandardColumnId.JobID,
        "annotation_preferenc.es/foo-alpha",
        StandardColumnId.Queue,
        StandardColumnId.Namespace,
        StandardColumnId.JobSet,
        StandardColumnId.State,
        StandardColumnId.Count,
        StandardColumnId.Priority,
        StandardColumnId.Owner,
        StandardColumnId.CPU,
        StandardColumnId.Memory,
        StandardColumnId.EphemeralStorage,
        StandardColumnId.GPU,
        StandardColumnId.PriorityClass,
        StandardColumnId.LastTransitionTimeUtc,
        StandardColumnId.TimeInState,
        StandardColumnId.TimeSubmittedUtc,
        StandardColumnId.TimeSubmittedAgo,
        StandardColumnId.Node,
        StandardColumnId.Cluster,
        StandardColumnId.ExitCode,
        StandardColumnId.RuntimeSeconds,
      ],
      order: { id: StandardColumnId.TimeInState, direction: "ASC" },
      pageIndex: 5,
      pageSize: 20,
      sidebarJobId: "223344",
      visibleColumns: {},
    }

    ensurePreferencesAreConsistent(validPreferences)

    const expected: JobsTablePreferences = {
      annotationColumnKeys: ["preferenc.es/foo-alpha"],
      expandedState: {},
      filters: [
        { id: "annotation_preferenc.es/foo-alpha", value: "aaa" },
        {
          id: StandardColumnId.State,
          value: ["QUEUED", "PENDING", "RUNNING"],
        },
      ],
      columnMatches: { "annotation_preferenc.es/foo-alpha": Match.StartsWith, [StandardColumnId.State]: Match.AnyOf },
      groupedColumns: [StandardColumnId.Queue, "annotation_preferenc.es/foo-alpha", StandardColumnId.JobSet],
      columnOrder: [
        StandardColumnId.JobID,
        "annotation_preferenc.es/foo-alpha",
        StandardColumnId.Queue,
        StandardColumnId.Namespace,
        StandardColumnId.JobSet,
        StandardColumnId.State,
        StandardColumnId.Count,
        StandardColumnId.Priority,
        StandardColumnId.Owner,
        StandardColumnId.CPU,
        StandardColumnId.Memory,
        StandardColumnId.EphemeralStorage,
        StandardColumnId.GPU,
        StandardColumnId.PriorityClass,
        StandardColumnId.LastTransitionTimeUtc,
        StandardColumnId.TimeInState,
        StandardColumnId.TimeSubmittedUtc,
        StandardColumnId.TimeSubmittedAgo,
        StandardColumnId.Node,
        StandardColumnId.Cluster,
        StandardColumnId.ExitCode,
        StandardColumnId.RuntimeSeconds,
      ],
      order: { id: StandardColumnId.TimeInState, direction: "ASC" },
      pageIndex: 5,
      pageSize: 20,
      sidebarJobId: "223344",
      // All added
      visibleColumns: {
        "annotation_preferenc.es/foo-alpha": true,
        jobSet: true,
        queue: true,
        state: true,
        timeInState: true,
      },
    }

    expect(validPreferences).toEqual(expected)
  })

  it("adds to the column order includes all unpinned standard columns and annotations and removes any columns which are neither", () => {
    const validPreferences: JobsTablePreferences = {
      annotationColumnKeys: ["preferenc.es/foo-alpha", "preferenc.es/foo-bravo", "preferenc.es/foo-charlie"],
      expandedState: {},
      filters: [],
      columnMatches: {},
      groupedColumns: [],
      columnOrder: [
        StandardColumnId.TimeInState,
        "annotation_rubbish-annotation",
        "annotation_preferenc.es/foo-bravo",
        "rubbish" as ColumnId,
        StandardColumnId.JobID,
      ],
      order: { id: StandardColumnId.TimeInState, direction: "ASC" },
      pageIndex: 5,
      pageSize: 20,
      sidebarJobId: "223344",
      visibleColumns: {
        [StandardColumnId.JobID]: true,
        [StandardColumnId.TimeInState]: true,
        'annotation_"preferenc.es/foo-bravo': true,
      },
    }

    ensurePreferencesAreConsistent(validPreferences)

    const expected: JobsTablePreferences = {
      annotationColumnKeys: ["preferenc.es/foo-alpha", "preferenc.es/foo-bravo", "preferenc.es/foo-charlie"],
      expandedState: {},
      filters: [],
      columnMatches: {},
      groupedColumns: [],
      columnOrder: [
        StandardColumnId.TimeInState,
        "annotation_preferenc.es/foo-bravo",
        StandardColumnId.JobID,

        // All the following elements are added
        "annotation_preferenc.es/foo-alpha",
        "annotation_preferenc.es/foo-charlie",
        StandardColumnId.Queue,
        StandardColumnId.Namespace,
        StandardColumnId.JobSet,
        StandardColumnId.State,
        StandardColumnId.Count,
        StandardColumnId.Priority,
        StandardColumnId.Owner,
        StandardColumnId.CPU,
        StandardColumnId.Memory,
        StandardColumnId.EphemeralStorage,
        StandardColumnId.GPU,
        StandardColumnId.PriorityClass,
        StandardColumnId.LastTransitionTimeUtc,
        StandardColumnId.TimeSubmittedUtc,
        StandardColumnId.TimeSubmittedAgo,
        StandardColumnId.Node,
        StandardColumnId.Cluster,
        StandardColumnId.ExitCode,
        StandardColumnId.RuntimeSeconds,
      ],
      order: { id: StandardColumnId.TimeInState, direction: "ASC" },
      pageIndex: 5,
      pageSize: 20,
      sidebarJobId: "223344",
      visibleColumns: {
        [StandardColumnId.JobID]: true,
        [StandardColumnId.TimeInState]: true,
        'annotation_"preferenc.es/foo-bravo': true,
      },
    }

    expect(validPreferences).toEqual(expected)
  })
})
