import { QueryClientProvider } from "@tanstack/react-query"
import { render, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { SnackbarProvider } from "notistack"

import { Sidebar } from "./Sidebar"
import { queryClient } from "../../../App"
import { Job, JobRunState, JobState } from "../../../models/lookoutV2Models"
import { FakeServicesProvider } from "../../../services/fakeContext"
import { FakeCordonService } from "../../../services/lookoutV2/mocks/FakeCordonService"
import FakeGetJobInfoService from "../../../services/lookoutV2/mocks/FakeGetJobInfoService"
import { FakeGetRunInfoService } from "../../../services/lookoutV2/mocks/FakeGetRunInfoService"
import { makeTestJob } from "../../../utils/fakeJobsUtils"

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
    onClose = vi.fn()
  })

  const renderComponent = () =>
    render(
      <SnackbarProvider>
        <QueryClientProvider client={queryClient}>
          <FakeServicesProvider fakeJobs={[job]}>
            <Sidebar
              job={job}
              runInfoService={new FakeGetRunInfoService()}
              jobSpecService={new FakeGetJobInfoService()}
              cordonService={new FakeCordonService()}
              sidebarWidth={600}
              onClose={onClose}
              onWidthChange={() => undefined}
              commandSpecs={[]}
            />
          </FakeServicesProvider>
        </QueryClientProvider>
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
    await userEvent.click(getByRole("tab", { name: /Result/ }))

    // First run should already be expanded
    within(getByRole("row", { name: /Run ID/ })).getByText(run.runId)
    within(getByRole("row", { name: /Cluster/ })).getByText(run.cluster)
  })

  it("should handle runs with errors", async () => {
    const { getByRole } = renderComponent()
    const run = job.runs[0]
    run.exitCode = 137

    // Switch to runs tab
    await userEvent.click(getByRole("tab", { name: /Result/ }))

    // First run should already be expanded
    within(getByRole("row", { name: /Run ID/ })).getByText(run.runId)
    within(getByRole("row", { name: /Exit code/ })).getByText(137)
  })

  it("should handle no runs", async () => {
    job.runs = []
    const { getByRole, getByText } = renderComponent()

    // Switch to runs tab
    await userEvent.click(getByRole("tab", { name: /Result/ }))

    getByText("This job did not run.")
  })
})
