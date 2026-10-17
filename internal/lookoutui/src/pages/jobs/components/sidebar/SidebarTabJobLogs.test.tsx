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

  const run = (runId: string, cluster: string, podName: string | undefined): JobRun => ({
    jobId: "job-1",
    runId,
    cluster,
    podName,
    jobRunState: JobRunState.RunRunning,
  })

  const renderComponent = (runs: JobRun[]) => {
    const job = makeTestJob("queue", "job-set", "job-1", JobState.Running, undefined, runs)
    return render(
      <SnackbarProvider>
        <QueryClientProvider client={new QueryClient()}>
          <ApiClientsProvider>
            <SidebarTabJobLogs job={job} />
          </ApiClientsProvider>
        </QueryClientProvider>
      </SnackbarProvider>,
    )
  }

  it("reads the logs of the pod that Lookout stores for the latest run", async () => {
    const { findByText } = renderComponent([run("run-1", "cluster-1", "armada-run-1")])

    await findByText("logs of armada-run-1")
    expect(logRequests[0]).toEqual({
      cluster: "cluster-1",
      body: {
        podName: "armada-run-1",
        podNamespace: "team-a",
        sinceTime: "",
        logOptions: { container: "main", tailLines: 1000 },
      },
    })
  })

  it("reads the pod on the cluster of the selected run", async () => {
    const { findByText, getByRole } = renderComponent([
      run("run-1", "cluster-1", "armada-run-1"),
      run("run-2", "cluster-2", "armada-run-2"),
    ])
    await findByText("logs of armada-run-2")

    await userEvent.click(getByRole("combobox", { name: /Job Run/ }))
    await userEvent.click(within(getByRole("listbox")).getByText(/Run 2/))

    await findByText("logs of armada-run-1")
    expect(logRequests.at(-1)).toMatchObject({ cluster: "cluster-1", body: { podName: "armada-run-1" } })
  })

  it("shows no logs for a run without a pod", async () => {
    const { findByText, container } = renderComponent([run("run-1", "cluster-1", undefined)])

    await findByText("This run has no pod, so it has no logs.")
    // The container comes from the job spec, so the tab has all other inputs of a request. An enabled query would
    // show the loading skeleton in the same render.
    await findByText("main")
    expect(container.querySelector(".MuiSkeleton-root")).toBeNull()
    expect(logRequests).toEqual([])
  })
})
