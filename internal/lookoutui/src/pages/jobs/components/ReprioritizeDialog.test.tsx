import { QueryClientProvider } from "@tanstack/react-query"
import { render, screen, waitFor } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { SnackbarProvider } from "notistack"

import { queryClient } from "../../../app/App"
import { makeManyTestJobs } from "../../../common/fakeJobsUtils"
import { Job, JobFiltersWithExcludes, JobState, Match } from "../../../models/lookoutModels"
import { ApiClientsProvider } from "../../../services/apiClients"
import { MockServer } from "../../../services/lookout/mocks/mockServer"
import {
  FORMAT_NUMBER_SHOULD_FORMAT_KEY,
  FORMAT_TIMESTAMP_SHOULD_FORMAT_KEY,
} from "../../../userSettings/localStorageKeys"

import { ReprioritizeDialog } from "./ReprioritizeDialog"

const mockServer = new MockServer()

describe("ReprioritizeDialog", () => {
  const numJobs = 5
  const numFinishedJobs = 0
  let jobs: Job[]
  let selectedItemFilters: JobFiltersWithExcludes[]
  let onClose: () => void

  beforeEach(() => {
    localStorage.clear()
    localStorage.setItem(FORMAT_NUMBER_SHOULD_FORMAT_KEY, JSON.stringify(false))
    localStorage.setItem(FORMAT_TIMESTAMP_SHOULD_FORMAT_KEY, JSON.stringify(false))

    jobs = makeManyTestJobs(numJobs, numFinishedJobs)
    selectedItemFilters = [
      {
        jobFilters: [
          {
            field: "jobId",
            value: "job-id-0",
            match: Match.Exact,
          },
        ],
        excludesJobFilters: [],
      },
    ]

    mockServer.listen()

    onClose = vi.fn()
  })

  afterEach(() => {
    localStorage.clear()
    mockServer.reset()
  })

  afterAll(() => {
    mockServer.close()
  })

  const renderComponent = () =>
    render(
      <QueryClientProvider client={queryClient}>
        <ApiClientsProvider>
          <SnackbarProvider>
            <ReprioritizeDialog onClose={onClose} selectedItemFilters={selectedItemFilters} />
          </SnackbarProvider>
        </ApiClientsProvider>
      </QueryClientProvider>,
    )

  it("displays job information", async () => {
    mockServer.setPostJobsResponse([jobs[0]])
    const { getByRole, findByRole, getByText } = renderComponent()

    // Initial render
    getByRole("heading", { name: "Reprioritize jobs" })

    // Once job details are fetched
    await findByRole("heading", { name: "Reprioritize 1 job" })

    // Check basic job information is displayed
    getByText("job-id-0")
    getByText("queue-0")
    getByText("job-set-0")
  })

  it("shows an alert if all jobs in a terminated state", async () => {
    mockServer.setPostJobsResponse([{ ...jobs[0], state: JobState.Failed }])
    const { findByText } = renderComponent()

    // Once job details are fetched
    await findByText(/All selected jobs are in a terminated state already/i)
  })

  it("paginates through many jobs correctly", async () => {
    jobs = makeManyTestJobs(6000, 6000 - 1480)
    selectedItemFilters = [
      {
        jobFilters: [
          {
            field: "queue",
            value: "queue-0",
            match: Match.Exact,
          },
        ],
        excludesJobFilters: [],
      },
    ]
    mockServer.setPostJobsResponse(jobs)

    const { findByRole, getByText } = renderComponent()

    // 6000 total jobs, split between 2 queues = 3000 jobs per queue
    // But only a subset will be in a non-terminated state
    // These will always be the same numbers as long as the makeJobs random seed is the same
    await findByRole("heading", { name: "Reprioritize 1480 jobs" }, { timeout: 3000 })
    getByText("6000 jobs are selected, but only 1480 jobs are in a non-terminated state.")
  })

  it.each([
    {
      method: "clicking the button",
      action: async (getByRole: ReturnType<typeof render>["getByRole"]) => {
        const reprioritizeButton = await waitFor(() => getByRole("button", { name: /Reprioritize 1 job/i }))
        await userEvent.click(reprioritizeButton)
      },
    },
    {
      method: "pressing Enter",
      action: async () => {
        await userEvent.keyboard("{Enter}")
      },
    },
  ])("allows the user to reprioritize jobs by $method", async ({ action }) => {
    mockServer.setPostJobsResponse([jobs[0]])
    const { getByRole, findByText } = renderComponent()

    mockServer.setReprioritizeJobsResponse([jobs[0].jobId], [])

    await enterPriority("2")

    await action(getByRole)

    await findByText(/Successfully changed priority./i)
  })

  it("does not allow the user to enter an invalid priority", async () => {
    const { getByRole } = renderComponent()
    await enterPriority("abc")
    expect(getByRole("button", { name: /Reprioritize/ })).toBeDisabled()
  })

  it("allows user to refetch jobs", async () => {
    mockServer.setPostJobsResponse([jobs[0]])
    const { findByText, findByRole } = renderComponent()

    // Check job details are being shown
    await findByText(jobs[0].jobId)
    await findByRole("cell", { name: jobs[0].priority.toString() })

    // Verify the job doesn't already have the priority we'll use to test
    expect(jobs[0].priority).not.toBe(1234)

    // Now change the job's priority on the "backend"
    jobs[0].priority = 1234

    // User clicks refetch
    await userEvent.click(await findByRole("button", { name: /Refetch jobs/i }))

    // Verify the state was re-fetched and updated
    await findByText(jobs[0].jobId)
    await findByRole("cell", { name: "1234" })
  })

  it("shows error reasons if reprioritization fails", async () => {
    const { getByRole, findByText } = renderComponent()

    mockServer.setReprioritizeJobsResponse([], [{ jobId: jobs[0].jobId, errorReason: "This is a test" }])

    await enterPriority("3")

    const reprioritizeButton = await waitFor(() => getByRole("button", { name: /Reprioritize 1 job/i }))
    await userEvent.click(reprioritizeButton)

    // Snackbar popup
    await findByText(/All jobs failed to reprioritize/i)

    // Verify reason is shown in table
    await findByText("This is a test", {}, { timeout: 3000 })
  })

  it("handles a partial success", async () => {
    // Select 2 jobs
    selectedItemFilters = [
      {
        jobFilters: [
          {
            field: "jobId",
            value: jobs[0].jobId,
            match: Match.Exact,
          },
        ],
        excludesJobFilters: [],
      },
      {
        jobFilters: [
          {
            field: "jobId",
            value: jobs[1].jobId,
            match: Match.Exact,
          },
        ],
        excludesJobFilters: [],
      },
    ]

    // Update mock to return both jobs
    mockServer.setPostJobsResponse([jobs[0], { ...jobs[1], state: JobState.Pending }])

    const { getByRole, findByText, findByRole } = renderComponent()

    // Fail 1, succeed the other
    mockServer.setReprioritizeJobsResponse([jobs[0].jobId], [{ jobId: jobs[1].jobId, errorReason: "This is a test" }])

    await enterPriority("0")

    const reprioritizeButton = await waitFor(() => getByRole("button", { name: /Reprioritize 2 jobs/i }))
    await userEvent.click(reprioritizeButton)

    // Snackbar popup
    await findByText(/Some jobs failed to reprioritize/i)

    // Verify reason is shown in table
    await findByText("Success", {}, { timeout: 3000 })
    await findByText("This is a test")

    // This job was successfully updated
    jobs[0].priority = 0

    // Check the user can re-attempt the request after a refetch
    await userEvent.click(getByRole("button", { name: /Refetch jobs/i }))
    waitFor(async () => {
      expect(await findByRole("button", { name: /Reprioritize 2 jobs/i })).toBeEnabled()
    })
  })

  async function enterPriority(priority: string) {
    const priorityTextBox = await screen.findByRole("textbox", { name: "New priority for jobs" })
    await userEvent.clear(priorityTextBox)
    await userEvent.type(priorityTextBox, priority)
  }
})
