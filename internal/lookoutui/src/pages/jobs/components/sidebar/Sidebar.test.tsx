import { QueryClientProvider } from "@tanstack/react-query"
import { render, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { SnackbarProvider } from "notistack"

import { queryClient } from "../../../../app/App"
import { makeTestJob } from "../../../../common/fakeJobsUtils"
import { Job, JobRunState, JobState } from "../../../../models/lookoutModels"
import { ApiClientsProvider } from "../../../../services/apiClients"
import { MockServer } from "../../../../services/lookout/mocks/mockServer"

import { Sidebar } from "./Sidebar"

const mockServer = new MockServer()

describe("Sidebar", () => {
  let job: Job, onClose: () => undefined

  beforeAll(() => {
    mockServer.listen()
  })

  beforeEach(() => {
    job = makeTestJob(
      "test-queue",
      "test-job-set",
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
    mockServer.setPostJobRunErrorResponseForRunId("1234-5678", "job run error")
    mockServer.setPostJobRunDebugMessageResponseForRunId("1234-5678", "job run debug message")
    mockServer.setPostJobSpecResponse({
      // eslint-disable-next-line @cspell/spellchecker
      clientId: "01gvgjbr0jrzvschp2f8jhk6n5",
      // eslint-disable-next-line @cspell/spellchecker
      jobSetId: "alices-project-0",
      queue: "alice",
      namespace: "default",
      owner: "anonymous",
      podSpec: {
        containers: [
          {
            name: "cpu-burner",
            // eslint-disable-next-line @cspell/spellchecker
            image: "containerstack/alpine-stress:latest",
            command: ["sh"],
            args: ["-c", "echo FAILED && echo hello world > /dev/termination-log && exit 137"],
            resources: {
              limits: { cpu: "200m", "ephemeral-storage": "8Gi", memory: "128Mi", "nvidia.com/gpu": "8" },
              requests: { cpu: "200m", "ephemeral-storage": "8Gi", memory: "128Mi", "nvidia.com/gpu": "8" },
            },
            imagePullPolicy: "IfNotPresent",
          },
        ],
        restartPolicy: "Never",
        terminationGracePeriodSeconds: 1,
        tolerations: [
          // eslint-disable-next-line @cspell/spellchecker
          { key: "armadaproject.io/armada", operator: "Equal", value: "true", effect: "NoSchedule" },
          // eslint-disable-next-line @cspell/spellchecker
          { key: "armadaproject.io/pc-armada-default", operator: "Equal", value: "true", effect: "NoSchedule" },
        ],
        priorityClassName: "armada-default",
      },
      created: "2023-03-14T17:23:21.29874Z",
    })
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
            <Sidebar job={job} sidebarWidth={600} onClose={onClose} onWidthChange={() => undefined} commandSpecs={[]} />
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
