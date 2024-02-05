import { render, screen, waitFor } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { Job, JobFilter, JobState, Match } from "models/lookoutV2Models"
import { SnackbarProvider } from "notistack"
import { IGetJobsService } from "services/lookoutV2/GetJobsService"
import { UpdateJobsResponse, UpdateJobsService } from "services/lookoutV2/UpdateJobsService"
import FakeGetJobsService from "services/lookoutV2/mocks/FakeGetJobsService"
import { makeManyTestJobs } from "utils/fakeJobsUtils"

import { ReprioritiseDialog } from "./ReprioritiseDialog"

describe("ReprioritiseDialog", () => {
  const numJobs = 5
  const numFinishedJobs = 0
  let jobs: Job[],
    selectedItemFilters: JobFilter[][],
    getJobsService: IGetJobsService,
    updateJobsService: UpdateJobsService,
    onClose: () => void

  beforeEach(() => {
    jobs = makeManyTestJobs(numJobs, numFinishedJobs)
    selectedItemFilters = [
      [
        {
          field: "jobId",
          value: "job-id-0",
          match: Match.Exact,
        },
      ],
    ]
    getJobsService = new FakeGetJobsService(jobs)
    updateJobsService = {
      reprioritiseJobs: jest.fn(),
    } as any
    onClose = jest.fn()
  })

  const renderComponent = () =>
    render(
      <SnackbarProvider>
        <ReprioritiseDialog
          onClose={onClose}
          selectedItemFilters={selectedItemFilters}
          getJobsService={getJobsService}
          updateJobsService={updateJobsService}
        />
      </SnackbarProvider>,
    )

  it("displays job information", async () => {
    const { getByRole, findByRole, getByText } = renderComponent()

    // Initial render
    getByRole("heading", { name: "Reprioritise jobs" })

    // Once job details are fetched
    await findByRole("heading", { name: "Reprioritise 1 job" })

    // Check basic job information is displayed
    getByText("job-id-0")
    getByText("queue-0")
    getByText("job-set-0")
  })

  it("shows an alert if all jobs in a terminated state", async () => {
    jobs[0].state = JobState.Failed
    const { findByText } = renderComponent()

    // Once job details are fetched
    await findByText(/All selected jobs are in a terminated state already/i)
  })

  it("paginates through many jobs correctly", async () => {
    jobs = makeManyTestJobs(6000, 6000 - 1480)
    selectedItemFilters = [
      [
        {
          field: "queue",
          value: "queue-0",
          match: Match.Exact,
        },
      ],
    ]
    getJobsService = new FakeGetJobsService(jobs)

    const { findByRole, getByText } = renderComponent()

    // 6000 total jobs, split between 2 queues = 3000 jobs per queue
    // But only a subset will be in a non-terminated state
    // These will always be the same numbers as long as the makeJobs random seed is the same
    await findByRole("heading", { name: "Reprioritise 1480 jobs" }, { timeout: 3000 })
    getByText("6000 jobs are selected, but only 1480 jobs are in a non-terminated state.")
  })

  it("allows the user to reprioritise jobs", async () => {
    const { getByRole, findByText } = renderComponent()

    updateJobsService.reprioritiseJobs = jest.fn((): Promise<UpdateJobsResponse> => {
      return Promise.resolve({
        successfulJobIds: [jobs[0].jobId],
        failedJobIds: [],
      })
    })

    await enterPriority("2")

    const cancelButton = await waitFor(() => getByRole("button", { name: /Reprioritise 1 job/i }))
    await userEvent.click(cancelButton)

    await findByText(/Successfully changed priority./i)
  })

  it("does not allow the user to enter an invalid priority", async () => {
    const { getByRole } = renderComponent()
    await enterPriority("abc")
    expect(getByRole("button", { name: /Reprioritise/ })).toBeDisabled()
  })

  it("allows user to refetch jobs", async () => {
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

  it("shows error reasons if reprioritisation fails", async () => {
    const { getByRole, findByText } = renderComponent()

    updateJobsService.reprioritiseJobs = jest.fn((): Promise<UpdateJobsResponse> => {
      return Promise.resolve({
        successfulJobIds: [],
        failedJobIds: [{ jobId: jobs[0].jobId, errorReason: "This is a test" }],
      })
    })

    await enterPriority("3")

    const reprioritiseButton = await waitFor(() => getByRole("button", { name: /Reprioritise 1 job/i }))
    await userEvent.click(reprioritiseButton)

    // Snackbar popup
    await findByText(/All jobs failed to reprioritise/i)

    // Verify reason is shown in table
    await findByText("This is a test", {}, { timeout: 3000 })
  })

  it("handles a partial success", async () => {
    // Select 2 jobs
    selectedItemFilters = [
      [
        {
          field: "jobId",
          value: jobs[0].jobId,
          match: Match.Exact,
        },
      ],
      [
        {
          field: "jobId",
          value: jobs[1].jobId,
          match: Match.Exact,
        },
      ],
    ]

    // jobs[1] in the dataset is terminated, so lets just fix that for the test
    jobs[1].state = JobState.Pending

    const { getByRole, findByText, findByRole } = renderComponent()

    // Fail 1, succeed the other
    updateJobsService.reprioritiseJobs = jest.fn((): Promise<UpdateJobsResponse> => {
      return Promise.resolve({
        successfulJobIds: [jobs[0].jobId],
        failedJobIds: [{ jobId: jobs[1].jobId, errorReason: "This is a test" }],
      })
    })

    await enterPriority("0")

    const reprioritiseButton = await waitFor(() => getByRole("button", { name: /Reprioritise 2 jobs/i }))
    await userEvent.click(reprioritiseButton)

    // Snackbar popup
    await findByText(/Some jobs failed to reprioritise/i)

    // Verify reason is shown in table
    await findByText("Success", {}, { timeout: 3000 })
    await findByText("This is a test")

    // This job was successfully updated
    jobs[0].priority = 0

    // Check the user can re-attempt the request after a refetch
    await userEvent.click(getByRole("button", { name: /Refetch jobs/i }))
    expect(await findByRole("button", { name: /Reprioritise 2 jobs/i })).toBeEnabled()
  })

  async function enterPriority(priority: string) {
    const priorityTextBox = await screen.findByRole("textbox", { name: "New priority for jobs" })
    await userEvent.clear(priorityTextBox)
    await userEvent.type(priorityTextBox, priority)
  }
})
