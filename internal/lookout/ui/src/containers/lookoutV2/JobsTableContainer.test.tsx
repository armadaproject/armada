import { render, within, waitFor, waitForElementToBeRemoved, screen } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { Job, JobState } from "models/lookoutV2Models"
import { SnackbarProvider } from "notistack"
import { IGetJobsService } from "services/lookoutV2/GetJobsService"
import { IGroupJobsService } from "services/lookoutV2/GroupJobsService"
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
    updateJobsService: UpdateJobsService

  beforeEach(() => {
    numJobs = 5
    numQueues = 2
    numJobSets = 3
    jobs = makeTestJobs(numJobs, 1, numQueues, numJobSets)
    getJobsService = new FakeGetJobsService(jobs)
    groupJobsService = new FakeGroupJobsService(jobs)

    updateJobsService = {
      cancelJobs: jest.fn(),
    } as any
  })

  const renderComponent = () =>
    render(
      <SnackbarProvider>
        <JobsTableContainer
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
    const { findByText } = renderComponent()
    await waitForFinishedLoading()

    await findByText("There is no data to display")
    await findByText("0–0 of 0")
  })

  it("should show jobs by default", async () => {
    const { findByRole } = renderComponent()
    await waitForFinishedLoading()

    // Check all details for the first job are shown
    const jobToSearchFor = jobs[0]
    const matchingRow = await findByRole("row", { name: "jobId:" + jobToSearchFor.jobId })

    within(matchingRow).getByText(jobToSearchFor.jobId)
    within(matchingRow).getByText(jobToSearchFor.jobSet)
    within(matchingRow).getByText(jobToSearchFor.queue)
    within(matchingRow).getByText(formatJobState(jobToSearchFor.state))
    within(matchingRow).getByText(formatUtcDate(jobToSearchFor.submitted))

    await assertNumDataRowsShown(jobs.length)
  })

  describe("Grouping", () => {
    it.each([
      ["Job Set", "jobSet"],
      ["Queue", "queue"],
      ["State", "state"],
    ])("should allow grouping by %s", async (displayString, groupKey) => {
      const jobObjKey = groupKey as keyof Job

      const numUniqueForJobKey = new Set(jobs.map((j) => j[jobObjKey])).size

      renderComponent()
      await waitForFinishedLoading()

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

    it("should allow 2 level grouping", async () => {
      jobs = makeTestJobs(6, 1, numQueues, numJobSets)
      getJobsService = new FakeGetJobsService(jobs)
      groupJobsService = new FakeGroupJobsService(jobs)

      renderComponent()
      await waitForFinishedLoading()

      // Group to both levels
      await groupByColumn("Queue")
      await groupByColumn("Job Set")
      await assertNumDataRowsShown(numQueues)

      const job = jobs[1] // Pick the second job as a bit of variation

      // Expand the first level
      await expandRow(job.queue)
      await assertNumDataRowsShown(numQueues + numJobSets)

      // Expand the second level
      await expandRow(job.jobSet)
      await assertNumDataRowsShown(numQueues + numJobSets + 1)
    })

    it("should allow 3 level grouping", async () => {
      jobs = makeTestJobs(1000, 1, numQueues, numJobSets)
      getJobsService = new FakeGetJobsService(jobs)
      groupJobsService = new FakeGroupJobsService(jobs)

      const numStates = new Set(jobs.map((j) => j.state)).size

      renderComponent()
      await waitForFinishedLoading()

      // Group to 3 levels
      await groupByColumn("State")
      await groupByColumn("Job Set")
      await groupByColumn("Queue")
      await assertNumDataRowsShown(numStates)

      const job = jobs[0]

      // Expand the first level
      await expandRow(job.state)
      await assertNumDataRowsShown(numStates + numJobSets)

      // Expand the second level
      await expandRow(job.jobSet)
      await assertNumDataRowsShown(numStates + numJobSets + numQueues)

      // Expand the third level
      await expandRow(job.queue)
      const numJobsExpectedToShow = jobs.filter(
        (j) => j.state === job.state && j.jobSet === job.jobSet && j.queue === job.queue,
      ).length
      await assertNumDataRowsShown(numStates + numJobSets + numQueues + numJobsExpectedToShow)
    })

    it("should reset currently-expanded if grouping changes", async () => {
      jobs = makeTestJobs(5, 1, numQueues, numJobSets)
      getJobsService = new FakeGetJobsService(jobs)
      groupJobsService = new FakeGroupJobsService(jobs)

      const { getByRole, queryAllByRole } = renderComponent()
      await waitForFinishedLoading()

      await groupByColumn("Queue")

      // Check we're only showing one row for each queue
      await assertNumDataRowsShown(numQueues)

      // Expand a row
      const job = jobs[0]
      await expandRow(job.queue)

      // Check the row right number of rows is being shown
      const numShownJobs = jobs.filter((j) => j.queue === job.queue).length
      await assertNumDataRowsShown(numQueues + numShownJobs)

      // Assert arrow down icon is shown
      getByRole("button", { name: "Collapse row" })

      // Group by another header
      await groupByColumn("Job Set")

      // Verify all rows are now collapsed
      await waitFor(() => expect(queryAllByRole("button", { name: "Collapse row" }).length).toBe(0))
    })
  })

  describe("Selecting", () => {
    it("should allow selecting of jobs", async () => {
      const { findByRole } = renderComponent()
      await waitForFinishedLoading()

      expect(await findByRole("button", { name: "Cancel selected" })).toBeDisabled()
      expect(await findByRole("button", { name: "Reprioritize selected" })).toBeDisabled()

      await toggleSelectedRow("jobId", jobs[0].jobId)
      await toggleSelectedRow("jobId", jobs[2].jobId)

      expect(await findByRole("button", { name: "Cancel selected" })).toBeEnabled()
      expect(await findByRole("button", { name: "Cancel selected" })).toBeEnabled()

      await toggleSelectedRow("jobId", jobs[2].jobId)

      expect(await findByRole("button", { name: "Cancel selected" })).toBeEnabled()
      expect(await findByRole("button", { name: "Cancel selected" })).toBeEnabled()

      await toggleSelectedRow("jobId", jobs[0].jobId)

      expect(await findByRole("button", { name: "Cancel selected" })).toBeDisabled()
      expect(await findByRole("button", { name: "Cancel selected" })).toBeDisabled()
    })
  })

  describe("Cancelling", () => {
    it("should pass individual jobs to cancel dialog", async () => {
      jobs[0].state = JobState.Pending

      const { findByRole } = renderComponent()
      await waitForFinishedLoading()

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

      await groupByColumn("Queue")

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
      await assertNumDataRowsShown(jobs.length)

      await filterTextColumnTo("Queue", jobs[0].queue)
      await assertNumDataRowsShown(jobs.filter((j) => j.queue === jobs[0].queue).length)

      await filterTextColumnTo("Job ID", jobs[0].jobId)
      await assertNumDataRowsShown(1)

      await filterTextColumnTo("Queue", "")
      await filterTextColumnTo("Job ID", "")

      await assertNumDataRowsShown(jobs.length)
    })

    it("should allow enum filtering", async () => {
      renderComponent()
      await waitForFinishedLoading()
      await assertNumDataRowsShown(jobs.length)

      await toggleEnumFilterOption("State", formatJobState(jobs[0].state))
      await assertNumDataRowsShown(2)

      await toggleEnumFilterOption("State", formatJobState(jobs[0].state))
      await assertNumDataRowsShown(jobs.length)
    })
  })

  describe("Sorting", () => {
    it("should allow sorting jobs", async () => {
      const { getAllByRole } = renderComponent()
      await waitForFinishedLoading()

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

    // Commented out until sorting by group name is supported
    // it("should allow sorting groups", async () => {
    //   const { getAllByRole, getByRole } = renderComponent()
    //   await waitForFinishedLoading()

    //   await groupByColumn("Queue")
    //   await assertNumDataRowsShown(numQueues)

    //   await toggleSorting("Queue")

    //   await waitFor(() => {
    //     const rows = getAllByRole("row")
    //     // Skipping header and footer rows
    //     expect(rows[1]).toHaveTextContent("queue-1")
    //     expect(rows[rows.length - 2]).toHaveTextContent("queue-2")
    //   })

    //   await toggleSorting("Queue")

    //   await waitFor(() => {
    //     const rows = getAllByRole("row")

    //     // Order should be reversed now
    //     expect(rows[1]).toHaveTextContent("queue-2")
    //     expect(rows[rows.length - 2]).toHaveTextContent("queue-1")
    //   })
    // })
  })

  describe("Refreshing data", () => {
    it("should allow refreshing data", async () => {
      const { findByRole } = renderComponent()
      await waitForFinishedLoading()
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
      await assertNumDataRowsShown(numJobs)

      // Applying grouping and filtering
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

  async function waitForFinishedLoading() {
    await waitForElementToBeRemoved(() => screen.getAllByRole("progressbar"))
  }

  async function assertNumDataRowsShown(nDataRows: number) {
    await waitFor(
      async () => {
        const rows = await screen.findAllByRole("row")
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

  async function expandRow(buttonText: string) {
    const rowToExpand = await screen.findByRole("row", {
      name: new RegExp(buttonText),
    })
    const expandButton = within(rowToExpand).getByRole("button", { name: "Expand row" })
    await userEvent.click(expandButton)
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
    const filterInput = await within(headerCell).findByRole("textbox", { name: "Filter" })
    await userEvent.clear(filterInput)
    if (filterText.length > 0) {
      await userEvent.type(filterInput, filterText)
    }
  }

  async function toggleEnumFilterOption(columnDisplayName: string, filterOption: string) {
    const headerCell = await getHeaderCell(columnDisplayName)
    const dropdownTrigger = await within(headerCell).findByRole("button", { name: "Filter" })
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
})
