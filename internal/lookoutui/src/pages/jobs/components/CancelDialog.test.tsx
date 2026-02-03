import { QueryClientProvider } from "@tanstack/react-query"
import { render, waitFor } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { SnackbarProvider } from "notistack"

import { queryClient } from "../../../app/App"
import { makeManyTestJobs } from "../../../common/fakeJobsUtils"
import { formatJobState } from "../../../common/jobsTableFormatters"
import { Job, JobFiltersWithExcludes, JobState, Match } from "../../../models/lookoutModels"
import { ApiClientsProvider } from "../../../services/apiClients"
import { MockServer } from "../../../services/lookout/mocks/mockServer"
import {
  FORMAT_NUMBER_SHOULD_FORMAT_KEY,
  FORMAT_TIMESTAMP_SHOULD_FORMAT_KEY,
} from "../../../userSettings/localStorageKeys"

import { CancelDialog } from "./CancelDialog"

const mockServer = new MockServer()

describe("CancelDialog", () => {
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
            <CancelDialog onClose={onClose} selectedItemFilters={selectedItemFilters} />
          </SnackbarProvider>
        </ApiClientsProvider>
      </QueryClientProvider>,
    )

  it("displays job information", async () => {
    mockServer.setPostJobsResponse([jobs[0]])
    const { getByRole, findByRole, getByText } = renderComponent()

    // Initial render
    getByRole("heading", { name: "Cancel jobs" })

    // Once job details are fetched
    await findByRole("heading", { name: "Cancel 1 job" })

    // Check basic job information is displayed
    getByText("job-id-0")
    getByText("queue-0")
    getByText("job-set-0")
  })

  it("shows an alert if all jobs in a terminated state", async () => {
    const jobToReturn = { ...jobs[0], state: JobState.Failed }
    mockServer.setPostJobsResponse([jobToReturn])
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
    await findByRole("heading", { name: "Cancel 1480 jobs" }, { timeout: 3000 })
    getByText("6000 jobs are selected, but only 1480 jobs are in a cancellable (non-terminated) state.")
  })

  it.each([
    {
      method: "clicking the button",
      action: async (getByRole: ReturnType<typeof render>["getByRole"]) => {
        const cancelButton = await waitFor(() => getByRole("button", { name: /Cancel 1 job/i }))
        await userEvent.click(cancelButton)
      },
    },
    {
      method: "pressing Enter",
      action: async () => {
        await userEvent.keyboard("{Enter}")
      },
    },
  ])("allows the user to cancel jobs by $method", async ({ action }) => {
    mockServer.setPostJobsResponse([jobs[0]])
    const { getByRole, findByText } = renderComponent()

    mockServer.setCancelJobsResponse([jobs[0].jobId], [])

    await action(getByRole)

    await findByText(/Successfully began cancellation/i)
  })

  it("allows user to refetch jobs", async () => {
    mockServer.setPostJobsResponse([jobs[0]])
    const { findByText, findByRole } = renderComponent()

    // Check job details are being shown
    await findByText(jobs[0].jobId)
    await findByText(formatJobState(jobs[0].state))

    // Verify the job isn't already cancelled (fixed random seed means this will always be true)
    expect(jobs[0].state).not.toBe("Pending")

    // Now change the job's state
    jobs[0].state = JobState.Pending

    // User clicks refetch
    await userEvent.click(await findByRole("button", { name: /Refetch jobs/i }))

    // Verify the state was re-fetched and updated
    await findByText(jobs[0].jobId)
    await findByText(formatJobState(jobs[0].state))
  })

  it("shows error reasons if cancellation fails", async () => {
    mockServer.setPostJobsResponse(jobs)
    const { getByRole, findByText } = renderComponent()

    mockServer.setCancelJobsResponse([], [jobs[0].jobId])

    const cancelButton = await waitFor(() => getByRole("button", { name: /Cancel 1 job/i }))
    await userEvent.click(cancelButton)

    // Snackbar popup
    await findByText(/All jobs failed to cancel/i)

    // Verify reason is shown in table
    // Longer timeout since another fetch call is made before this is shown
    await findByText("failed to cancel job", {}, { timeout: 3000 })
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

    const firstJob = { ...jobs[0] }
    const secondJob = { ...jobs[1], state: JobState.Pending }
    mockServer.setPostJobsResponse([firstJob, secondJob])

    const { getByRole, findByText, findByRole } = renderComponent()

    // Fail 1, succeed the other
    mockServer.setCancelJobsResponse([firstJob.jobId], [secondJob.jobId])

    const cancelButton = await waitFor(() => getByRole("button", { name: /Cancel 2 jobs/i }))
    await userEvent.click(cancelButton)

    // Snackbar popup
    await findByText(/Some jobs failed to cancel/i)

    // Verify reason is shown in table
    // Longer timeout since another fetch call is made before this is shown
    await findByText("Success", {}, { timeout: 3000 })
    await findByText("failed to cancel job")

    // This job was successfully cancelled
    mockServer.setPostJobsResponse([{ ...firstJob, state: JobState.Cancelled }, secondJob])

    // Check the user can re-attempt the other job after a refetch
    await userEvent.click(getByRole("button", { name: /Refetch jobs/i }))
    expect(await findByRole("button", { name: /Cancel 1 job/i })).toBeEnabled()
  })
})
