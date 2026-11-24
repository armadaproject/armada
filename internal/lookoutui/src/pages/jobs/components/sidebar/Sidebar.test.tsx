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
    // Differs from the run's pending time (now) so a Submitted row showing the wrong time is caught
    job.submitted = "2020-06-15T12:00:00Z"
    mockServer.setPostJobRunErrorResponseForRunId("1234-5678", "job run error")
    mockServer.setPostJobRunDebugMessageResponseForRunId("1234-5678", { errorString: "job run debug message" })
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
    // The query client is shared across tests, so drop what one test fetched before the next test asks again
    queryClient.clear()
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
    // The time is shown with the user's timestamp settings, so only the year is stable here
    within(getByRole("row", { name: /Submitted/ })).getByText(/2020/)
  })

  it("should show when each run of a retried job started waiting in the queue", async () => {
    // The job is submitted in 2020, and its first run is preempted in 2022 and requeued, so the wait for the second
    // run is measured from then and not from submission
    job.runs = [
      {
        jobId: "abcde",
        runId: "1234-5678",
        cluster: "demo-a",
        leased: "2021-06-15T12:00:00Z",
        finished: "2022-06-15T12:00:00Z",
        jobRunState: JobRunState.RunPreempted,
      },
      {
        jobId: "abcde",
        runId: "9999-0000",
        cluster: "demo-b",
        leased: "2023-06-15T12:00:00Z",
        jobRunState: JobRunState.RunLeased,
      },
    ]
    const { getByRole, queryByRole } = renderComponent()

    await userEvent.click(getByRole("tab", { name: /Result/ }))

    // The newest run is expanded first
    within(getByRole("row", { name: /Requeued/ })).getByText(/2022/)
    expect(queryByRole("row", { name: /Submitted/ })).toBeNull()

    await userEvent.click(getByRole("button", { name: /Run 1/ }))
    within(getByRole("row", { name: /Submitted/ })).getByText(/2020/)
  })

  it("should measure each run's wait from the end of the run before it", async () => {
    // Each run is requeued when the one before it ends, so a later run must not use the first run's end
    job.runs = [
      {
        jobId: "abcde",
        runId: "1111-1111",
        cluster: "demo-a",
        leased: "2021-06-15T12:00:00Z",
        finished: "2022-06-15T12:00:00Z",
        jobRunState: JobRunState.RunPreempted,
      },
      {
        jobId: "abcde",
        runId: "2222-2222",
        cluster: "demo-a",
        leased: "2023-06-15T12:00:00Z",
        finished: "2024-06-15T12:00:00Z",
        jobRunState: JobRunState.RunPreempted,
      },
      {
        jobId: "abcde",
        runId: "3333-3333",
        cluster: "demo-a",
        leased: "2025-06-15T12:00:00Z",
        jobRunState: JobRunState.RunLeased,
      },
    ]
    const { getByRole, getAllByRole } = renderComponent()

    await userEvent.click(getByRole("tab", { name: /Result/ }))
    await userEvent.click(getByRole("button", { name: /Run 2/ }))
    await userEvent.click(getByRole("button", { name: /Run 1/ }))

    // Runs are listed newest first
    const requeuedRows = getAllByRole("row", { name: /Requeued/ })
    expect(requeuedRows).toHaveLength(2)
    within(requeuedRows[0]).getByText(/2024/)
    within(requeuedRows[1]).getByText(/2022/)
    within(getByRole("row", { name: /Submitted/ })).getByText(/2020/)
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

  it("should show the pod events table when the run has structured debug info", async () => {
    mockServer.setPostJobRunDebugMessageResponseForRunId("1234-5678", {
      errorString: "{}",
      debugInfo: {
        schemaVersion: 1,
        trigger: "cancelledNotStarted",
        pod: {
          events: [
            {
              type: "Warning",
              reason: "FailedMount",
              from: "kubelet",
              message: "MountVolume.SetUp failed",
              timestamp: "2026-11-03T18:59:35Z",
            },
          ],
        },
        node: { name: "node-1", _exists: false },
      },
    })
    const { getByRole, findByRole } = renderComponent()

    await userEvent.click(getByRole("tab", { name: /Result/ }))
    await userEvent.click(await findByRole("button", { name: "Debug" }))

    const eventsTable = await findByRole("table", { name: "Pod events" })
    // The time is shown with the user's timestamp settings, like the run's own times, so only the year is stable here
    within(eventsTable).getByRole("row", { name: /Warning.*FailedMount.*2026.*kubelet.*MountVolume.SetUp failed/ })

    // A field the generated model renames (exists -> _exists) must survive the trip through the mocked API
    await userEvent.click(getByRole("button", { name: "Node" }))
    within(await findByRole("row", { name: /Exists/ })).getByText("false")
  })

  it("should show an unstructured debug message as text", async () => {
    const { getByRole, findByRole, findByText, queryByRole } = renderComponent()

    await userEvent.click(getByRole("tab", { name: /Result/ }))
    await userEvent.click(await findByRole("button", { name: "Debug" }))

    expect(await findByText("job run debug message")).toBeInTheDocument()
    expect(queryByRole("table", { name: "Pod events" })).toBeNull()
  })

  it("should handle no runs", async () => {
    job.runs = []
    const { getByRole, getByText } = renderComponent()

    // Switch to runs tab
    await userEvent.click(getByRole("tab", { name: /Result/ }))

    getByText("This job did not run.")
  })
})
