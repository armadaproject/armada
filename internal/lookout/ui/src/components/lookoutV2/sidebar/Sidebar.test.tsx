import { render, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { Job, JobRunState, JobState } from "models/lookoutV2Models"
import { SnackbarProvider } from "notistack"
import { makeTestJob } from "utils/fakeJobsUtils"

import FakeGetJobSpecService from "../../../services/lookoutV2/mocks/FakeGetJobSpecService"
import { FakeGetRunErrorService } from "../../../services/lookoutV2/mocks/FakeGetRunErrorService"
import { FakeLogService } from "../../../services/lookoutV2/mocks/FakeLogService"
import { Sidebar } from "./Sidebar"

describe("Sidebar", () => {
  let job: Job, onClose: () => undefined

  beforeEach(() => {
    job = makeTestJob(
      "test-queue",
      "test-jobset",
      "abcde",
      JobState.Succeeded,
      {
        cpu: 3900,
        memory: 38 * 1024 ** 2,
        ephemeralStorage: 64 * 1024 ** 3,
        gpu: 4,
      },
      [
        {
          jobId: "abcde",
          runId: "1234-5678",
          cluster: "demo-a",
          pending: new Date().toISOString(),
          jobRunState: JobRunState.RunPending,
        },
      ],
    )
    onClose = jest.fn()
  })

  const renderComponent = () =>
    render(
      <SnackbarProvider>
        <Sidebar
          job={job}
          runErrorService={new FakeGetRunErrorService()}
          jobSpecService={new FakeGetJobSpecService()}
          logService={new FakeLogService()}
          sidebarWidth={600}
          onClose={onClose}
          onWidthChange={() => undefined}
        />
      </SnackbarProvider>,
    )

  it("should show job details by default", () => {
    const { getByRole } = renderComponent()

    within(getByRole("row", { name: /Queue/ })).getByText(job.queue)
    within(getByRole("row", { name: /Job Set/ })).getByText(job.jobSet)
    within(getByRole("row", { name: /CPU/ })).getByText("3.9")
    within(getByRole("row", { name: /Memory/ })).getByText("38Mi")
    within(getByRole("row", { name: /Ephemeral storage/ })).getByText("64Gi")
    within(getByRole("row", { name: /GPU/ })).getByText("4")
  })

  it("should allow users to view run details", async () => {
    const { getByRole } = renderComponent()
    const run = job.runs[0]

    // Switch to runs tab
    await userEvent.click(getByRole("tab", { name: /Runs/ }))

    // First run should already be expanded
    within(getByRole("row", { name: /Run ID/ })).getByText(run.runId)
    within(getByRole("row", { name: /Cluster/ })).getByText(run.cluster)
  })

  it("should handle runs with errors", async () => {
    const { getByRole } = renderComponent()
    const run = job.runs[0]
    run.exitCode = 137

    // Switch to runs tab
    await userEvent.click(getByRole("tab", { name: /Runs/ }))

    // First run should already be expanded
    within(getByRole("row", { name: /Run ID/ })).getByText(run.runId)
    within(getByRole("row", { name: /Exit code/ })).getByText(137)
  })

  it("should handle no runs", async () => {
    job.runs = []
    const { getByRole, getByText } = renderComponent()

    // Switch to runs tab
    await userEvent.click(getByRole("tab", { name: /Runs/ }))

    getByText("This job has not run.")
  })
})
