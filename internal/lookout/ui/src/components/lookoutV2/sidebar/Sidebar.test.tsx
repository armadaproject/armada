import { render, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { Job } from "models/lookoutV2Models"
import { SnackbarProvider } from "notistack"
import { makeRandomJobs } from "utils/fakeJobsUtils"

import FakeGetJobSpecService from "../../../services/lookoutV2/mocks/FakeGetJobSpecService"
import { FakeGetRunErrorService } from "../../../services/lookoutV2/mocks/FakeGetRunErrorService"
import { Sidebar } from "./Sidebar"

describe("Sidebar", () => {
  let job: Job, onClose: () => undefined

  beforeEach(() => {
    job = makeRandomJobs(1, 1, 1, 1)[0]
    onClose = jest.fn()
  })

  const renderComponent = () =>
    render(
      <SnackbarProvider>
        <Sidebar
          job={job}
          runErrorService={new FakeGetRunErrorService()}
          jobSpecService={new FakeGetJobSpecService()}
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
    within(getByRole("row", { name: /Memory/ })).getByText("128 MiB")
  })

  it("should allow users to view run details", async () => {
    const { getByRole } = renderComponent()
    const run = job.runs[0]

    // Switch to runs tab
    await userEvent.click(getByRole("tab", { name: /Runs/ }))

    // First run should already be expanded
    within(getByRole("row", { name: /Run ID/ })).getByText(run.runId)
    within(getByRole("row", { name: /Exit code/ })).getByText("17")
  })

  it("should handle runs with no errors", async () => {
    const { getByRole } = renderComponent()
    const run = job.runs[0]
    run.exitCode = undefined

    // Switch to runs tab
    await userEvent.click(getByRole("tab", { name: /Runs/ }))

    // First run should already be expanded
    within(getByRole("row", { name: /Run ID/ })).getByText(run.runId)
  })

  it("should handle no runs", async () => {
    job.runs = []
    const { getByRole, getByText } = renderComponent()

    // Switch to runs tab
    await userEvent.click(getByRole("tab", { name: /Runs/ }))

    getByText("This job has not run.")
  })
})
