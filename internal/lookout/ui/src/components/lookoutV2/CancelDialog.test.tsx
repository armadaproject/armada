import { render, waitFor } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { Job, JobFilter, JobState, Match } from "models/lookoutV2Models"
import { SnackbarProvider } from "notistack"
import { IGetJobsService } from "services/lookoutV2/GetJobsService"
import { UpdateJobsResponse, UpdateJobsService } from "services/lookoutV2/UpdateJobsService"
import FakeGetJobsService from "services/lookoutV2/mocks/FakeGetJobsService"
import { makeManyTestJobs } from "utils/fakeJobsUtils"
import { formatJobState } from "utils/jobsTableFormatters"

import { CancelDialog } from "./CancelDialog"

describe("CancelDialog", () => {
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
      cancelJobs: jest.fn(),
    } as any
    onClose = jest.fn()
  })

  const renderComponent = () =>
    render(
      <SnackbarProvider>
        <CancelDialog
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
    getByRole("heading", { name: "Cancel jobs" })

    // Once job details are fetched
    await findByRole("heading", { name: "Cancel 1 job" })

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
    await findByRole("heading", { name: "Cancel 1480 jobs" }, { timeout: 3000 })
    getByText("6000 jobs are selected, but only 1480 jobs are in a cancellable (non-terminated) state.")
  })

  it("allows the user to cancel jobs", async () => {
    const { getByRole, findByText } = renderComponent()

    updateJobsService.cancelJobs = jest.fn((): Promise<UpdateJobsResponse> => {
      return Promise.resolve({
        successfulJobIds: [jobs[0].jobId],
        failedJobIds: [],
      })
    })

    const cancelButton = await waitFor(() => getByRole("button", { name: /Cancel 1 job/i }))
    await userEvent.click(cancelButton)

    await findByText(/Successfully began cancellation/i)

    expect(updateJobsService.cancelJobs).toHaveBeenCalled()
  })

  it("allows user to refetch jobs", async () => {
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
    const { getByRole, findByText } = renderComponent()

    updateJobsService.cancelJobs = jest.fn((): Promise<UpdateJobsResponse> => {
      return Promise.resolve({
        successfulJobIds: [],
        failedJobIds: [{ jobId: jobs[0].jobId, errorReason: "This is a test" }],
      })
    })

    const cancelButton = await waitFor(() => getByRole("button", { name: /Cancel 1 job/i }))
    await userEvent.click(cancelButton)

    // Snackbar popup
    await findByText(/All jobs failed to cancel/i)

    // Verify reason is shown in table
    // Longer timeout since another fetch call is made before this is shown
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
    updateJobsService.cancelJobs = jest.fn((): Promise<UpdateJobsResponse> => {
      return Promise.resolve({
        successfulJobIds: [jobs[0].jobId],
        failedJobIds: [{ jobId: jobs[1].jobId, errorReason: "This is a test" }],
      })
    })

    const cancelButton = await waitFor(() => getByRole("button", { name: /Cancel 2 jobs/i }))
    await userEvent.click(cancelButton)

    // Snackbar popup
    await findByText(/Some jobs failed to cancel/i)

    // Verify reason is shown in table
    // Longer timeout since another fetch call is made before this is shown
    await findByText("Success", {}, { timeout: 3000 })
    await findByText("This is a test")

    // This job was successfully cancelled
    jobs[0].state = JobState.Cancelled

    // Check the user can re-attempt the other job after a refetch
    await userEvent.click(getByRole("button", { name: /Refetch jobs/i }))
    expect(await findByRole("button", { name: /Cancel 1 job/i })).toBeEnabled()
  })
})
