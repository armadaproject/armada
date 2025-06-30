import { QueryClientProvider } from "@tanstack/react-query"
import { render, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { SnackbarProvider } from "notistack"

import { Sidebar } from "./Sidebar"
import { queryClient } from "../../../App"
import { Job, JobRunState, JobState } from "../../../models/lookoutModels"
import { ApiClientsProvider } from "../../../services/apiClients"
import { FakeServicesProvider } from "../../../services/fakeContext"
import { MockServer } from "../../../services/lookout/mocks/mockServer"
import { makeTestJob } from "../../../utils/fakeJobsUtils"

const mockServer = new MockServer()

describe("Sidebar", () => {
  let job: Job, onClose: () => undefined

  beforeAll(() => {
    mockServer.listen()
  })

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

  afterEach(() => {
    mockServer.reset()
  })

  afterAll(() => {
    mockServer.close()
  })

  const renderComponent = () =>
    render(
      <SnackbarProvider>
        <QueryClientProvider client={queryClient}>
          <ApiClientsProvider>
            <FakeServicesProvider fakeJobs={[job]}>
              <Sidebar
                job={job}
                sidebarWidth={600}
                onClose={onClose}
                onWidthChange={() => undefined}
                commandSpecs={[]}
              />
            </FakeServicesProvider>
          </ApiClientsProvider>
        </QueryClientProvider>
      </SnackbarProvider>,
    )

  it("should show job details by default", async () => {
    const { findByRole } = renderComponent()

    within(await findByRole("row", { name: /Queue/ })).getByText(job.queue)
    within(await findByRole("row", { name: /Job Set/ })).getByText(job.jobSet)
    within(await findByRole("row", { name: /CPU/ })).getByText("3.9")
    within(await findByRole("row", { name: /Memory/ })).getByText("38Mi")
    within(await findByRole("row", { name: /Ephemeral storage/ })).getByText("64Gi")
    within(await findByRole("row", { name: /GPU/ })).getByText("4")
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
