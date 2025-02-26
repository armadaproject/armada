import { StandardColumnId, toAnnotationColId } from "./jobsTableColumns"
import { diffOfKeys, getFiltersForRowsSelection } from "./jobsTableUtils"
import { RowId } from "./reactTableUtils"
import { LookoutColumnFilter } from "../containers/lookout/JobsTableContainer"
import { JobFilter, JobFiltersWithExcludes, JobState, Match } from "../models/lookoutModels"

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

  describe("getFiltersForRowsSelection", () => {
    const testColumnFilters: LookoutColumnFilter[] = [
      {
        id: StandardColumnId.Namespace,
        value: "my-testing-",
      },
      { id: toAnnotationColId("testing.co.uk/abc"), value: "easy-as-123" },
    ]

    const testColumnMatches: Record<string, Match> = {
      [StandardColumnId.Namespace]: Match.StartsWith,
      [toAnnotationColId("testing.co.uk/abc")]: Match.Exact,
    }

    const expectedJobFiltersForColumns: JobFilter[] = [
      {
        isAnnotation: false,
        field: StandardColumnId.Namespace,
        value: "my-testing-",
        match: Match.StartsWith,
      },
      {
        isAnnotation: true,
        field: "testing.co.uk/abc",
        value: "easy-as-123",
        match: Match.Exact,
      },
    ]

    it("zero rows", () => {
      const result = getFiltersForRowsSelection([], {}, testColumnFilters, testColumnMatches)
      expect(result).toEqual([])
    })

    it("flat, ungrouped list", () => {
      const allIds: RowId[] = ["jobId:000", "jobId:001", "jobId:002", "jobId:003"]

      const result = getFiltersForRowsSelection(
        allIds.map((rowId) => ({ rowId })),
        {
          "jobId:001": true,
          "jobId:002": true,
        },
        testColumnFilters,
        testColumnMatches,
      )

      const expected: JobFiltersWithExcludes[] = [
        {
          jobFilters: [
            ...expectedJobFiltersForColumns,
            {
              field: StandardColumnId.JobID,
              isAnnotation: false,
              match: Match.Exact,
              value: "001",
            },
          ],
          excludesJobFilters: [],
        },
        {
          jobFilters: [
            ...expectedJobFiltersForColumns,
            {
              field: StandardColumnId.JobID,
              isAnnotation: false,
              match: Match.Exact,
              value: "002",
            },
          ],
          excludesJobFilters: [],
        },
      ]
      expect(result).toEqual(expected)
    })

    it("single-level group, not expanded", () => {
      const result = getFiltersForRowsSelection(
        [
          {
            rowId: "queue:myqueue123",
            isGroup: true,
            groupedField: StandardColumnId.Queue,
            subRows: [],
            stateCounts: {
              [JobState.Running]: 2,
              [JobState.Succeeded]: 1,
            },
          },
        ],
        {
          "queue:myqueue123": true,
        },
        testColumnFilters,
        testColumnMatches,
      )

      const expected: JobFiltersWithExcludes[] = [
        {
          jobFilters: [
            ...expectedJobFiltersForColumns,
            {
              field: StandardColumnId.Queue,
              isAnnotation: false,
              match: Match.Exact,
              value: "myqueue123",
            },
          ],
          excludesJobFilters: [],
        },
      ]
      expect(result).toEqual(expected)
    })

    it("multi-level groups, some expanded", () => {
      const result = getFiltersForRowsSelection(
        [
          {
            rowId: "queue:q1",
            isGroup: true,
            groupedField: StandardColumnId.Queue,
            subRows: [
              {
                rowId: "queue:q1>jobSet:js1",
                isGroup: true,
                groupedField: StandardColumnId.JobSet,
                subRows: [
                  {
                    rowId: "queue:q1>jobSet:js1>state:RUNNING",
                    isGroup: true,
                    groupedField: StandardColumnId.State,
                    subRows: [{ rowId: "jobId:001" }, { rowId: "jobId:002" }],
                    stateCounts: {
                      [JobState.Running]: 4,
                    },
                  },
                  {
                    rowId: "queue:q1>jobSet:js1>state:FAILED",
                    isGroup: true,
                    groupedField: StandardColumnId.State,
                    subRows: [],
                    stateCounts: {
                      [JobState.Failed]: 4,
                    },
                  },
                  {
                    rowId: "queue:q1>jobSet:js1>state:CANCELLED",
                    isGroup: true,
                    groupedField: StandardColumnId.State,
                    subRows: [{ rowId: "jobId:009" }, { rowId: "jobId:010" }, { rowId: "jobId:011" }],
                    stateCounts: {
                      [JobState.Cancelled]: 3,
                    },
                  },
                ],
                stateCounts: {
                  [JobState.Running]: 4,
                  [JobState.Failed]: 4,
                  [JobState.Cancelled]: 3,
                },
              },
              {
                rowId: "queue:q1>jobSet:js2",
                isGroup: true,
                groupedField: StandardColumnId.JobSet,
                subRows: [],
                stateCounts: {
                  [JobState.Running]: 40,
                  [JobState.Succeeded]: 4,
                },
              },
              {
                rowId: "queue:q1>jobSet:js3",
                isGroup: true,
                groupedField: StandardColumnId.JobSet,
                subRows: [],
                stateCounts: {
                  [JobState.Preempted]: 12,
                },
              },
            ],
            stateCounts: {
              [JobState.Running]: 44,
              [JobState.Failed]: 4,
              [JobState.Cancelled]: 3,
              [JobState.Succeeded]: 4,
              [JobState.Preempted]: 12,
            },
          },
        ],
        {
          "queue:q1": true,
          "queue:q1>jobSet:js1": true,
          "queue:q1>jobSet:js1>state:RUNNING": true,
          "jobId:002": true,
          "queue:q1>jobSet:js1>state:FAILED": true,
          "jobId:010": true,
          "jobId:011": true,
          "queue:q1>jobSet:js3": true,
        },
        testColumnFilters,
        testColumnMatches,
      )

      const queueExpectedJobFilter = (queue: string): JobFilter => ({
        field: StandardColumnId.Queue,
        isAnnotation: false,
        match: Match.Exact,
        value: queue,
      })

      const jobSetExpectedJobFilter = (jobSet: string): JobFilter => ({
        field: StandardColumnId.JobSet,
        isAnnotation: false,
        match: Match.Exact,
        value: jobSet,
      })

      const jobStateExpectedJobFilter = (jobState: JobState): JobFilter => ({
        field: StandardColumnId.State,
        isAnnotation: false,
        match: Match.Exact,
        value: jobState,
      })

      const jobIdExpectedJobFilter = (jobId: string): JobFilter => ({
        field: StandardColumnId.JobID,
        isAnnotation: false,
        match: Match.Exact,
        value: jobId,
      })

      const expected: JobFiltersWithExcludes[] = [
        {
          jobFilters: [...expectedJobFiltersForColumns, queueExpectedJobFilter("q1")],
          excludesJobFilters: [
            [...expectedJobFiltersForColumns, queueExpectedJobFilter("q1"), jobSetExpectedJobFilter("js1")],
            [...expectedJobFiltersForColumns, queueExpectedJobFilter("q1"), jobSetExpectedJobFilter("js2")],
            [...expectedJobFiltersForColumns, queueExpectedJobFilter("q1"), jobSetExpectedJobFilter("js3")],
          ],
        },
        {
          jobFilters: [...expectedJobFiltersForColumns, queueExpectedJobFilter("q1"), jobSetExpectedJobFilter("js1")],
          excludesJobFilters: [
            [
              ...expectedJobFiltersForColumns,
              queueExpectedJobFilter("q1"),
              jobSetExpectedJobFilter("js1"),
              jobStateExpectedJobFilter(JobState.Running),
            ],
            [
              ...expectedJobFiltersForColumns,
              queueExpectedJobFilter("q1"),
              jobSetExpectedJobFilter("js1"),
              jobStateExpectedJobFilter(JobState.Failed),
            ],
            [
              ...expectedJobFiltersForColumns,
              queueExpectedJobFilter("q1"),
              jobSetExpectedJobFilter("js1"),
              jobStateExpectedJobFilter(JobState.Cancelled),
            ],
          ],
        },
        {
          jobFilters: [
            ...expectedJobFiltersForColumns,
            queueExpectedJobFilter("q1"),
            jobSetExpectedJobFilter("js1"),
            jobStateExpectedJobFilter(JobState.Running),
          ],
          excludesJobFilters: [
            [...expectedJobFiltersForColumns, jobIdExpectedJobFilter("001")],
            [...expectedJobFiltersForColumns, jobIdExpectedJobFilter("002")],
          ],
        },
        {
          jobFilters: [...expectedJobFiltersForColumns, jobIdExpectedJobFilter("002")],
          excludesJobFilters: [],
        },
        {
          jobFilters: [
            ...expectedJobFiltersForColumns,
            queueExpectedJobFilter("q1"),
            jobSetExpectedJobFilter("js1"),
            jobStateExpectedJobFilter(JobState.Failed),
          ],
          excludesJobFilters: [],
        },
        { jobFilters: [...expectedJobFiltersForColumns, jobIdExpectedJobFilter("010")], excludesJobFilters: [] },
        { jobFilters: [...expectedJobFiltersForColumns, jobIdExpectedJobFilter("011")], excludesJobFilters: [] },
        {
          jobFilters: [...expectedJobFiltersForColumns, queueExpectedJobFilter("q1"), jobSetExpectedJobFilter("js3")],
          excludesJobFilters: [],
        },
      ]
      expect(result).toEqual(expected)
    })
  })
})
