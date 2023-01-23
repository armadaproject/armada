import { render, within, waitFor, screen } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { History, createMemoryHistory } from "history"
import { Job, JobState } from "models/lookoutV2Models"
import { SnackbarProvider } from "notistack"
import { IGetJobsService } from "services/lookoutV2/GetJobsService"
import { IGroupJobsService } from "services/lookoutV2/GroupJobsService"
import { JobsTablePreferencesService } from "services/lookoutV2/JobsTablePreferencesService"
import { UpdateJobsService } from "services/lookoutV2/UpdateJobsService"
import FakeGetJobsService from "services/lookoutV2/mocks/FakeGetJobsService"
import FakeGroupJobsService from "services/lookoutV2/mocks/FakeGroupJobsService"
import { makeTestJobs } from "utils/fakeJobsUtils"
import { formatJobState, formatUtcDate } from "utils/jobsTableFormatters"

import { JobsTableContainer } from "./JobsTableContainer"

// This is quite a heavy component, and tests can timeout on a slower machine
jest.setTimeout(15_000)

describe("JobsTableContainer", () => {
  let numJobs: number, numQueues: number, numJobSets: number
  let jobs: Job[],
    getJobsService: IGetJobsService,
    groupJobsService: IGroupJobsService,
    updateJobsService: UpdateJobsService,
    historyService: History,
    jobsTablePreferencesService: JobsTablePreferencesService

  beforeEach(() => {
    numJobs = 5
    numQueues = 2
    numJobSets = 3
    jobs = makeTestJobs(numJobs, 1, numQueues, numJobSets)
    getJobsService = new FakeGetJobsService(jobs, false)
    groupJobsService = new FakeGroupJobsService(jobs, false)

    historyService = createMemoryHistory()
    jobsTablePreferencesService = new JobsTablePreferencesService(historyService)

    updateJobsService = {
      cancelJobs: jest.fn(),
    } as any
  })

  const renderComponent = () =>
    render(
      <SnackbarProvider>
        <JobsTableContainer
          jobsTablePreferencesService={jobsTablePreferencesService}
          getJobsService={getJobsService}
          groupJobsService={groupJobsService}
          updateJobsService={updateJobsService}
          debug={false}
        />
      </SnackbarProvider>,
    )

  it("should render a spinner while loading initially", async () => {
    getJobsService.getJobs = jest.fn(() => new Promise(() => undefined))
    const { findAllByRole } = renderComponent()
    await findAllByRole("progressbar")
  })

  it("should handle no data", async () => {
    getJobsService.getJobs = jest.fn(() =>
      Promise.resolve({
        jobs: [],
        count: 0,
      }),
    )
    groupJobsService.groupJobs = jest.fn(() =>
      Promise.resolve({
        groups: [],
        count: 0,
      }),
    )
    const { findByText } = renderComponent()
    await waitForFinishedLoading()

    await findByText("There is no data to display")
    await findByText("0–0 of 0")
  })

  describe("Grouping", () => {
    it("should be grouped by queue+jobset by default", async () => {
      jobs = makeTestJobs(6, 1, numQueues, numJobSets)
      getJobsService = new FakeGetJobsService(jobs, false)
      groupJobsService = new FakeGroupJobsService(jobs, false)

      const { findByRole } = renderComponent()
      await waitForFinishedLoading()

      await assertNumDataRowsShown(numQueues)

      const job = jobs[1] // Pick the second job as a bit of variation

      // Expand the first level
      await expandRow(job.queue)
      await assertNumDataRowsShown(numQueues + numJobSets)

      // Expand the second level
      await expandRow(job.jobSet)
      await assertNumDataRowsShown(numQueues + numJobSets + 1)

      // Should be able to see job-level information
      const matchingRow = await findByRole("row", { name: "jobId:" + job.jobId })
      within(matchingRow).getByText(job.jobId)
      within(matchingRow).getByText(job.jobSet)
      within(matchingRow).getByText(job.queue)
      within(matchingRow).getByText(formatJobState(job.state))
      within(matchingRow).getByText(formatUtcDate(job.submitted))
    })

    it.each([
      ["Job Set", "jobSet"],
      ["Queue", "queue"],
      ["State", "state"],
    ])("should allow grouping by %s", async (displayString, groupKey) => {
      const jobObjKey = groupKey as keyof Job

      const numUniqueForJobKey = new Set(jobs.map((j) => j[jobObjKey])).size

      renderComponent()
      await waitForFinishedLoading()

      await clearAllGroupings()

      await groupByColumn(displayString)

      // Check number of rendered rows has changed
      await assertNumDataRowsShown(numUniqueForJobKey)

      // Expand a row
      const job = jobs[0]
      await expandRow(job[jobObjKey]!.toString()) // eslint-disable-line @typescript-eslint/no-non-null-assertion

      // Check the row right number of rows is being shown
      const numShownJobs = jobs.filter((j) => j[jobObjKey] === job[jobObjKey]).length
      await assertNumDataRowsShown(numUniqueForJobKey + numShownJobs)
    })

    it("should allow 3 level grouping", async () => {
      jobs = makeTestJobs(1000, 1, numQueues, numJobSets)
      getJobsService = new FakeGetJobsService(jobs, false)
      groupJobsService = new FakeGroupJobsService(jobs, false)

      const numStates = new Set(jobs.map((j) => j.state)).size

      renderComponent()
      await waitForFinishedLoading()

      // Group to 3 levels
      await groupByColumn("State")
      await assertNumDataRowsShown(numQueues)

      const job = jobs[0]

      // Expand the first level
      await expandRow(job.queue)
      await assertNumDataRowsShown(numQueues + numJobSets)

      // Expand the second level
      await expandRow(job.jobSet)
      await assertNumDataRowsShown(numQueues + numJobSets + numStates)

      // Expand the third level
      await expandRow(job.state)
      const numJobsExpectedToShow = jobs.filter(
        (j) => j.state === job.state && j.jobSet === job.jobSet && j.queue === job.queue,
      ).length
      await assertNumDataRowsShown(numStates + numJobSets + numQueues + numJobsExpectedToShow)
    })

    it("should reset currently-expanded if grouping changes", async () => {
      jobs = makeTestJobs(5, 1, numQueues, numJobSets)
      getJobsService = new FakeGetJobsService(jobs, false)
      groupJobsService = new FakeGroupJobsService(jobs, false)

      const { getByRole, queryAllByRole } = renderComponent()
      await waitForFinishedLoading()

      // Check we're only showing one row for each queue
      await assertNumDataRowsShown(numQueues)

      // Expand a row
      const job = jobs[0]
      await expandRow(job.queue)

      // Check the row right number of rows is being shown
      await assertNumDataRowsShown(numQueues + numJobSets)

      // Assert arrow down icon is shown
      getByRole("button", { name: "Collapse row" })

      // Group by another header
      await groupByColumn("State")

      // Verify all rows are now collapsed
      await waitFor(() => expect(queryAllByRole("button", { name: "Collapse row" }).length).toBe(0))
    })
  })

  describe("Selecting", () => {
    it("should allow selecting rows", async () => {
      const { findByRole } = renderComponent()
      await waitForFinishedLoading()

      expect(await findByRole("button", { name: "Cancel selected" })).toBeDisabled()
      expect(await findByRole("button", { name: "Reprioritize selected" })).toBeDisabled()

      expect(jobs[0].queue).not.toBe(jobs[1].queue)
      await toggleSelectedRow("queue", jobs[0].queue)
      await toggleSelectedRow("queue", jobs[1].queue)

      expect(await findByRole("button", { name: "Cancel selected" })).toBeEnabled()
      expect(await findByRole("button", { name: "Cancel selected" })).toBeEnabled()

      await toggleSelectedRow("queue", jobs[1].queue)

      expect(await findByRole("button", { name: "Cancel selected" })).toBeEnabled()
      expect(await findByRole("button", { name: "Cancel selected" })).toBeEnabled()

      await toggleSelectedRow("queue", jobs[0].queue)

      expect(await findByRole("button", { name: "Cancel selected" })).toBeDisabled()
      expect(await findByRole("button", { name: "Cancel selected" })).toBeDisabled()
    })
  })

  describe("Cancelling", () => {
    it("should pass individual jobs to cancel dialog", async () => {
      jobs[0].state = JobState.Pending

      const { findByRole } = renderComponent()
      await waitForFinishedLoading()

      await expandRow(jobs[0].queue)
      await expandRow(jobs[0].jobSet)

      await toggleSelectedRow("jobId", jobs[0].jobId)

      await userEvent.click(await findByRole("button", { name: "Cancel selected" }))
      await findByRole("dialog", { name: "Cancel 1 job" }, { timeout: 2000 })
    })

    it("should pass groups to cancel dialog", async () => {
      numJobs = 1000 // Add enough jobs that it exercises grouping logic
      jobs = makeTestJobs(numJobs, 1, numQueues, numJobSets)
      getJobsService = new FakeGetJobsService(jobs)
      groupJobsService = new FakeGroupJobsService(jobs)

      const { findByRole } = renderComponent()
      await waitForFinishedLoading()

      // Wait for table to update
      await assertNumDataRowsShown(numQueues)

      // Select a queue
      await toggleSelectedRow("queue", jobs[0].queue)

      // Open the cancel dialog
      await userEvent.click(await findByRole("button", { name: "Cancel selected" }))

      // Check it retrieved the number of non-terminated jobs in this queue
      // Longer timeout as some fake API calls need to be made
      // Number of jobs will be static as long as the random seed above is static
      await findByRole("dialog", { name: "Cancel 258 jobs" }, { timeout: 2000 })
    })
  })

  describe("Filtering", () => {
    it("should allow text filtering", async () => {
      renderComponent()
      await waitForFinishedLoading()
      await assertNumDataRowsShown(numQueues)

      await filterTextColumnTo("Queue", jobs[0].queue)
      await assertNumDataRowsShown(1)

      await filterTextColumnTo("Queue", "")
      await filterTextColumnTo("Job ID", "")

      await assertNumDataRowsShown(numQueues)
    })

    it("should allow enum filtering", async () => {
      renderComponent()
      await waitForFinishedLoading()
      await clearAllGroupings()

      await assertNumDataRowsShown(jobs.length)

      await toggleEnumFilterOption("State", formatJobState(jobs[0].state))
      await assertNumDataRowsShown(2)

      await toggleEnumFilterOption("State", formatJobState(jobs[0].state))
      await assertNumDataRowsShown(jobs.length)
    })

    it("allows filtering on annotation columns", async () => {
      const { findByRole } = renderComponent()
      await waitForFinishedLoading()
      await clearAllGroupings()

      await addAnnotationColumn("hyperparameter")
      await assertNumDataRowsShown(numJobs)

      const testAnnotationValue = jobs[0].annotations["hyperparameter"]
      expect(testAnnotationValue).toBe("59052f3d-cfd1-4a3e-8f23-173f92764c3f")
      await findByRole("cell", { name: testAnnotationValue })

      await filterTextColumnTo("hyperparameter", testAnnotationValue)
      await assertNumDataRowsShown(1)
    })
  })

  describe("Sorting", () => {
    it("should allow sorting jobs", async () => {
      const { getAllByRole } = renderComponent()
      await waitForFinishedLoading()
      await clearAllGroupings()

      await toggleSorting("Job ID")

      await waitFor(() => {
        const rows = getAllByRole("row")
        // Skipping header and footer rows
        expect(rows[1]).toHaveTextContent("1") // Job ID
        expect(rows[rows.length - 2]).toHaveTextContent((numJobs - 1).toString())
      })

      await toggleSorting("Job ID")

      await waitFor(() => {
        const rows = getAllByRole("row")

        // Order should be reversed now
        expect(rows[1]).toHaveTextContent((numJobs - 1).toString())
        expect(rows[rows.length - 2]).toHaveTextContent("1") // Job ID
      })
    })
  })

  describe("Refreshing data", () => {
    it("should allow refreshing data", async () => {
      const { findByRole } = renderComponent()
      await waitForFinishedLoading()
      await clearAllGroupings()
      await assertNumDataRowsShown(numJobs)

      const firstRow = await findByRole("row", { name: new RegExp(jobs[0].jobId) })

      // Assert first job is not currently in Running state
      expect(within(firstRow).queryByRole("cell", { name: "Running" })).toBeNull()

      // Transition the job
      jobs[0].state = JobState.Running

      // Refresh the data
      await triggerRefresh()
      await waitForFinishedLoading()

      // Assert its now showing in the Running state
      expect(within(firstRow).queryByRole("cell", { name: "Running" })).not.toBeNull()
    })

    it("should maintain grouping and filtering state when refreshing", async () => {
      const { findByText } = renderComponent()
      await waitForFinishedLoading()

      // Applying grouping and filtering
      await clearAllGroupings()
      await groupByColumn("Queue")
      await filterTextColumnTo("Job ID", jobs[0].jobId)

      // Check table is updated as expected
      await assertNumDataRowsShown(1)
      await findByText("queue-1 (1)")

      // Refresh the data
      await triggerRefresh()
      await waitForFinishedLoading()

      // Check table is in the same state
      await assertNumDataRowsShown(1)
      await findByText("queue-1 (1)")
    })
  })

  describe("Sidebar", () => {
    it("clicking job row should open sidebar", async () => {
      const { getByRole } = renderComponent()
      await waitForFinishedLoading()

      const firstJob = jobs[0]
      await expandRow(firstJob.queue)
      await expandRow(firstJob.jobSet)

      await clickOnJobRow(firstJob.jobId)

      const sidebar = getByRole("complementary")
      within(sidebar).getByText(firstJob.jobId)
    })

    it("clicking grouped row should not open", async () => {
      const { findByRole, queryByRole } = renderComponent()
      await waitForFinishedLoading()

      const firstJob = jobs[0]
      const firstRow = await findByRole("row", { name: new RegExp(firstJob.queue) })
      await userEvent.click(within(firstRow).getByText(new RegExp(firstJob.queue)))

      expect(queryByRole("complementary")).toBeNull()
    })
  })

  describe("Query Params", () => {
    it("should save table state to query params on load", async () => {
      renderComponent()
      await waitForFinishedLoading()

      expect(historyService.location.search).toContain("page=0")
      expect(historyService.location.search).toContain("g[0]=queue&g[1]=jobSet")
      expect(historyService.location.search).toContain("sort[0][id]=jobId&sort[0][desc]=true")
    })

    it("should save modifications to query params", async () => {
      renderComponent()
      await waitForFinishedLoading()

      await clearAllGroupings()
      await groupByColumn("Job Set")

      await filterTextColumnTo("Job Set", jobs[0].jobSet)

      await expandRow(jobs[0].jobSet)

      await clickOnJobRow(jobs[0].jobId)

      expect(historyService.location.search).toContain("g[0]=jobSet")
      expect(historyService.location.search).not.toContain("g[1]")
      expect(historyService.location.search).toContain("sb=01gkv9cj53h0rk9407mds0")
      expect(historyService.location.search).toContain("e[0]=jobSet%3Ajob-set-1")
    })

    it("should populate table state from query params", async () => {
      // Set query param to the same as the test above
      historyService.push({
        ...historyService.location,
        search: `?page=0&g[0]=jobSet&sort[0][id]=jobId&sort[0][desc]=true&vCols[0]=jobId&vCols[1]=queue&vCols[2]=jobSet&vCols[3]=state&vCols[4]=timeSubmittedUtc&vCols[5]=timeInState&vCols[6]=selectorCol&pS=50&f[0][id]=jobSet&f[0][value]=job-set-1&e[0]=jobSet%3Ajob-set-1&sb=01gkv9cj53h0rk9407mds0`,
      })

      const { findByRole } = renderComponent()
      await waitForFinishedLoading()

      // 1 jobset + jobs for expanded jobset
      await assertNumDataRowsShown(1 + jobs.filter((j) => j.jobSet === "job-set-1").length)

      const sidebar = await findByRole("complementary")
      within(sidebar).getByText(jobs[0].jobId)
      within(sidebar).getByText(jobs[0].queue)
    })
  })

  async function waitForFinishedLoading() {
    await waitFor(() => expect(screen.queryAllByRole("progressbar").length).toBe(0))
  }

  async function assertNumDataRowsShown(nDataRows: number) {
    await waitFor(
      async () => {
        const table = await screen.findByRole("table", { name: "Jobs table" })
        const rows = await within(table).findAllByRole("row")
        expect(rows.length).toBe(nDataRows + 2) // One row per data row, plus the header and footer rows
      },
      { timeout: 3000 },
    )
  }

  async function groupByColumn(columnDisplayName: string) {
    const groupByDropdownButton = await screen.findByRole("button", { name: "Group by" })
    await userEvent.click(groupByDropdownButton)

    const dropdown = await screen.findByRole("listbox")
    const colToGroup = await within(dropdown).findByText(columnDisplayName)
    await userEvent.click(colToGroup)
  }

  async function clearAllGroupings() {
    const clearGroupingButtons = screen.queryAllByRole("button", { name: /Clear grouping/i })
    for (const clearGroupingButton of clearGroupingButtons) {
      await userEvent.click(clearGroupingButton)
    }
  }

  async function expandRow(buttonText: string) {
    const rowToExpand = await screen.findByRole("row", {
      name: new RegExp(buttonText),
    })
    const expandButton = within(rowToExpand).getByRole("button", { name: "Expand row" })
    await userEvent.click(expandButton)
    await waitForFinishedLoading()
  }

  async function toggleSelectedRow(rowType: string, rowId: string) {
    const matchingRow = await screen.findByRole("row", { name: `${rowType}:${rowId}` })
    const checkbox = await within(matchingRow).findByRole("checkbox")
    await userEvent.click(checkbox)
  }

  async function getHeaderCell(columnDisplayName: string) {
    return await screen.findByRole("columnheader", { name: columnDisplayName })
  }

  async function filterTextColumnTo(columnDisplayName: string, filterText: string) {
    const headerCell = await getHeaderCell(columnDisplayName)
    const filterInput = await within(headerCell).findByRole("textbox")
    await userEvent.clear(filterInput)
    if (filterText.length > 0) {
      await userEvent.type(filterInput, filterText)
    }
  }

  async function toggleEnumFilterOption(columnDisplayName: string, filterOption: string) {
    const headerCell = await getHeaderCell(columnDisplayName)
    const dropdownTrigger = await within(headerCell).findByRole("button", { name: "Filter..." })
    await userEvent.click(dropdownTrigger)
    const optionButton = await screen.findByRole("option", { name: filterOption })
    await userEvent.click(optionButton)

    // Ensure the dropdown is closed
    await userEvent.tab()
  }

  async function toggleSorting(columnDisplayName: string) {
    const headerCell = await getHeaderCell(columnDisplayName)
    const sortButton = await within(headerCell).findByRole("button", { name: "Toggle sort" })
    await userEvent.click(sortButton)
  }

  async function triggerRefresh() {
    const button = await screen.findByRole("button", { name: "Refresh" })
    await userEvent.click(button)
  }

  async function addAnnotationColumn(annotationKey: string) {
    const editColumnsButton = await screen.findByRole("button", { name: /columns selected/i })
    await userEvent.click(editColumnsButton)

    const addColumnButton = await screen.findByRole("button", { name: /Add column/i })
    await userEvent.click(addColumnButton)

    const textbox = await screen.findByRole("textbox", { name: /Annotation key/i })
    await userEvent.type(textbox, annotationKey)

    const saveButton = await screen.findByRole("button", { name: /Save/i })
    await userEvent.click(saveButton)

    // Close pop up
    await userEvent.click(screen.getByText(/Click here to add an annotation column/i))
    await userEvent.keyboard("{Escape}")
  }

  async function clickOnJobRow(jobId: string) {
    const jobRow = await screen.findByRole("row", { name: new RegExp(jobId) })
    await userEvent.click(within(jobRow).getByText(jobId))
  }
})
