import { VisibilityState } from "@tanstack/react-table"
import { describe, expect, it } from "vitest"

import { DEFAULT_COLUMN_VISIBILITY, StandardColumnId } from "../common/jobsTableColumns"
import { JobsTablePreferences } from "../services/lookout/JobsTablePreferencesService"

import { buildViewEventData } from "./viewMetadata"

function makePrefs(overrides: Partial<JobsTablePreferences> = {}): JobsTablePreferences {
  return {
    annotationColumnKeys: [],
    visibleColumns: { ...DEFAULT_COLUMN_VISIBILITY },
    columnOrder: [],
    groupedColumns: [],
    expandedState: {},
    pageIndex: 0,
    pageSize: 50,
    order: { id: "jobId", direction: "DESC" },
    columnSizing: {},
    filters: [],
    columnMatches: {},
    sidebarJobId: undefined,
    ...overrides,
  }
}

describe("buildViewEventData", () => {
  it("returns empty columns field for default preferences", () => {
    const result = buildViewEventData("my-view", makePrefs())
    expect(result.viewName).toBe("my-view")
    expect(result.columns).toBe("")
  })

  it("includes added columns without prefix", () => {
    const visibleColumns: VisibilityState = {
      ...DEFAULT_COLUMN_VISIBILITY,
      Node: true, // not in defaults
    }
    const result = buildViewEventData("test", makePrefs({ visibleColumns }))
    expect(result.columns).toContain("Node")
    expect(result.columns).not.toContain("-Node")
  })

  it("prefixes removed default columns with -", () => {
    const visibleColumns: VisibilityState = {
      ...DEFAULT_COLUMN_VISIBILITY,
      [StandardColumnId.Queue]: false, // Queue is in defaults
    }
    const result = buildViewEventData("test", makePrefs({ visibleColumns }))
    expect(result.columns).toContain(`-${StandardColumnId.Queue}`)
  })

  it("includes annotation columns", () => {
    const result = buildViewEventData(
      "test",
      makePrefs({
        annotationColumnKeys: ["team", "priority"],
      }),
    )
    expect(result.columns).toContain("annotation_team")
    expect(result.columns).toContain("annotation_priority")
    expect(result.hasAnnotations).toBe("true")
    expect(result.annotationCount).toBe("2")
  })

  it("reports filter count and filteredColumns correctly", () => {
    const result = buildViewEventData(
      "test",
      makePrefs({
        filters: [
          { id: "Queue", value: "test" },
          { id: "State", value: "Running" },
        ],
      }),
    )
    expect(result.filterCount).toBe("2")
    expect(result.filteredColumns).toBe("Queue,State")
  })

  it("reports grouped columns", () => {
    const result = buildViewEventData("test", makePrefs({ groupedColumns: [StandardColumnId.Queue, StandardColumnId.State] }))
    expect(result.groupedColumns).toBe("queue,state")
    expect(result.groupCount).toBe("2")
  })

  it("reports sort column and direction", () => {
    const result = buildViewEventData("test", makePrefs({ order: { id: "Queue", direction: "ASC" } }))
    expect(result.sortColumn).toBe("Queue")
    expect(result.sortDirection).toBe("ASC")
  })

  it("reports undefined autoRefresh and activeJobSets as unset", () => {
    const result = buildViewEventData("test", makePrefs({ autoRefresh: undefined, activeJobSets: undefined }))
    expect(result.autoRefresh).toBe("unset")
    expect(result.activeJobSets).toBe("unset")
  })

  it("reports boolean autoRefresh and activeJobSets", () => {
    const result = buildViewEventData("test", makePrefs({ autoRefresh: true, activeJobSets: false }))
    expect(result.autoRefresh).toBe("true")
    expect(result.activeJobSets).toBe("false")
  })

  it("reports pageSize", () => {
    const result = buildViewEventData("test", makePrefs({ pageSize: 100 }))
    expect(result.pageSize).toBe("100")
  })

  it("reports columnCount based on visible columns", () => {
    const visibleColumns: VisibilityState = {
      ...DEFAULT_COLUMN_VISIBILITY,
      Node: true,
      ExtraCol: true,
    }
    const result = buildViewEventData("test", makePrefs({ visibleColumns }))
    const expectedCount = Object.values(visibleColumns).filter(Boolean).length
    expect(result.columnCount).toBe(String(expectedCount))
  })
})
