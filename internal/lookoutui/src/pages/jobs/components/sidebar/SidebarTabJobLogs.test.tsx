import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { render, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { http, HttpResponse } from "msw"
import { SnackbarProvider } from "notistack"

import { makeTestJob } from "../../../../common/fakeJobsUtils"
import { JobRun, JobRunState, JobState } from "../../../../models/lookoutModels"
import { ApiClientsProvider } from "../../../../services/apiClients"
import { MockServer } from "../../../../services/lookout/mocks/mockServer"

import { SidebarTabJobLogs } from "./SidebarTabJobLogs"

const mockServer = new MockServer()

type LogRequest = { cluster: string; body: Record<string, unknown> }

const jobId = "01hcq4x7d2k9m3n5p8r0s1t2v4"

describe("SidebarTabJobLogs", () => {
  let logRequests: LogRequest[]
  // setupTests sets the UI config before every test file.
  const uiConfig = window.__LOOKOUT_UI_CONFIG__!
  let binocularsBaseUrlPattern: typeof uiConfig.binocularsBaseUrlPattern

  beforeAll(() => {
    mockServer.listen()
    // jsdom does not implement scrollIntoView, and the log view scrolls to the last line.
    Element.prototype.scrollIntoView = vi.fn()
    binocularsBaseUrlPattern = uiConfig.binocularsBaseUrlPattern
    uiConfig.binocularsBaseUrlPattern = "http://binoculars.{CLUSTER_ID}"
  })

  beforeEach(() => {
    logRequests = []
    mockServer.setPostJobSpecResponse({
      namespace: "team-a",
      podSpec: { containers: [{ name: "main" }] },
    })
    mockServer.use(
      http.post("*/v1/binoculars/log", async ({ request }) => {
        const body = (await request.json()) as Record<string, unknown>
        logRequests.push({ cluster: new URL(request.url).hostname.replace("binoculars.", ""), body })
        return HttpResponse.json({ log: [{ timestamp: "2026-01-01T00:00:00Z", line: `logs of ${body.podName}` }] })
      }),
    )
  })

  afterEach(() => {
    mockServer.reset()
  })

  afterAll(() => {
    mockServer.close()
    uiConfig.binocularsBaseUrlPattern = binocularsBaseUrlPattern
  })

  const run = (runId: string, cluster: string, podName?: string): JobRun => ({
    jobId,
    runId,
    cluster,
    podName,
    jobRunState: JobRunState.RunRunning,
  })

  const renderComponent = (runs: JobRun[]) => {
    const queryClient = new QueryClient()
    const tree = (jobRuns: JobRun[]) => (
      <SnackbarProvider>
        <QueryClientProvider client={queryClient}>
          <ApiClientsProvider>
            <SidebarTabJobLogs job={makeTestJob("queue", "job-set", jobId, JobState.Running, undefined, jobRuns)} />
          </ApiClientsProvider>
        </QueryClientProvider>
      </SnackbarProvider>
    )
    const result = render(tree(runs))
    return { ...result, rerenderWithRuns: (jobRuns: JobRun[]) => result.rerender(tree(jobRuns)) }
  }

  it("reads the logs of the pod that Lookout stores for the latest run", async () => {
    const { findByText } = renderComponent([run("run-1", "cluster-1", "armada-run-1")])

    await findByText("logs of armada-run-1")
    expect(logRequests[0]).toMatchObject({
      cluster: "cluster-1",
      body: { jobId, podNumber: 0, podName: "armada-run-1", podNamespace: "team-a" },
    })
  })

  it("reads the pod on the cluster of the selected run", async () => {
    const { findByText, getByRole } = renderComponent([
      run("run-1", "cluster-1", "armada-run-1"),
      run("run-2", "cluster-2", "armada-run-2"),
    ])
    await findByText("logs of armada-run-2")

    await userEvent.click(getByRole("combobox", { name: /Job Run/ }))
    await userEvent.click(within(getByRole("listbox")).getByText(/Run 1/))

    await findByText("logs of armada-run-1")
    expect(logRequests.at(-1)).toMatchObject({ cluster: "cluster-1", body: { podName: "armada-run-1" } })
  })

  it("keeps the selected run when a refresh adds a run", async () => {
    const runs = [run("run-1", "cluster-1", "armada-run-1"), run("run-2", "cluster-2", "armada-run-2")]
    const { findByText, getByRole, rerenderWithRuns } = renderComponent(runs)
    await findByText("logs of armada-run-2")
    await userEvent.click(getByRole("combobox", { name: /Job Run/ }))
    await userEvent.click(within(getByRole("listbox")).getByText(/Run 1/))
    await findByText("logs of armada-run-1")

    rerenderWithRuns([...runs, run("run-3", "cluster-3", "armada-run-3")])

    await findByText("logs of armada-run-1")
    expect(getByRole("combobox", { name: /Job Run/ })).toHaveTextContent("Run 1")
  })

  it("lets the user select another run while the selected run has no pod", async () => {
    const { findByText, getByRole } = renderComponent([
      run("run-1", "cluster-1", "armada-run-1"),
      run("run-2", "cluster-2"),
    ])
    await findByText("This run has no pod, so it has no logs.")

    await userEvent.click(getByRole("combobox", { name: /Job Run/ }))
    await userEvent.click(within(getByRole("listbox")).getByText(/Run 1/))

    await findByText("logs of armada-run-1")
  })

  it("shows no logs for a run without a pod", async () => {
    const { findByText, container } = renderComponent([run("run-1", "cluster-1")])

    await findByText("This run has no pod, so it has no logs.")
    // The container comes from the job spec, so the tab has all other inputs of a request. An enabled query would
    // show the loading skeleton in the same render.
    await findByText("main")
    expect(container.querySelector(".MuiSkeleton-root")).toBeNull()
    expect(logRequests).toEqual([])
  })
})
