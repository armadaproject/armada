import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { render } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { http, HttpResponse } from "msw"
import { SnackbarProvider } from "notistack"
import { createMemoryRouter, RouterProvider } from "react-router-dom"
import { v4 as uuidV4 } from "uuid"
import { vi } from "vitest"

import { Job, JobState } from "../../../models/lookoutModels"
import { JOB_SETS } from "../../../pathnames"
import { ApiClientsProvider } from "../../../services/apiClients"
import { MockServer } from "../../../services/lookout/mocks/mockServer"

import JobSetsContainer from "./JobSetsContainer"

const mockServer = new MockServer()

function makeTestJobs(n: number, queue: string, jobSet: string, state: JobState): Job[] {
  const jobs: Job[] = []
  for (let i = 0; i < n; i++) {
    jobs.push({
      annotations: {},
      cpu: 1,
      ephemeralStorage: 8192,
      gpu: 0,
      jobId: uuidV4(),
      jobSet,
      lastTransitionTime: "2024-01-01T00:00:00Z",
      memory: 8192,
      owner: queue,
      namespace: queue,
      priority: 1000,
      priorityClass: "armada-default",
      queue,
      runs: [],
      state,
      submitted: "2024-01-01T00:00:00Z",
    })
  }
  return jobs
}

describe("JobSetsContainer", () => {
  beforeAll(() => {
    mockServer.listen()
  })

  beforeEach(() => {
    localStorage.clear()
  })

  afterEach(() => {
    localStorage.clear()
    mockServer.reset()
  })

  afterAll(() => {
    mockServer.close()
  })

  const renderComponent = (search: string) => {
    const testQueryClient = new QueryClient({
      defaultOptions: {
        queries: { retry: false, gcTime: 0 },
      },
    })

    const element = (
      <SnackbarProvider maxSnack={10} autoHideDuration={null}>
        <QueryClientProvider client={testQueryClient}>
          <ApiClientsProvider>
            <JobSetsContainer jobSetsAutoRefreshMs={30000} />
          </ApiClientsProvider>
        </QueryClientProvider>
      </SnackbarProvider>
    )

    const router = createMemoryRouter(
      [
        {
          path: JOB_SETS,
          element,
        },
      ],
      { initialEntries: [JOB_SETS + search], initialIndex: 0 },
    )

    return render(<RouterProvider router={router} />)
  }

  it("renders the job sets table for the queue in the URL", async () => {
    mockServer.setPostJobsResponse([
      ...makeTestJobs(2, "queue-1", "job-set-a", JobState.Running),
      ...makeTestJobs(3, "queue-1", "job-set-b", JobState.Queued),
    ])

    const { findByLabelText, queryByText } = renderComponent("?queue=queue-1")

    // The "select all" header checkbox only renders when at least one job set has loaded.
    expect(await findByLabelText("select all job sets")).toBeInTheDocument()
    expect(queryByText("No job sets found for this queue.")).not.toBeInTheDocument()
  })

  it("refetches when the active-only filter changes", async () => {
    // Only finished jobs: visible by default, but excluded once "Active only" is on.
    mockServer.setPostJobsResponse(makeTestJobs(2, "queue-1", "finished-set", JobState.Succeeded))

    const { findByLabelText, findByText, getByLabelText } = renderComponent("?queue=queue-1")

    expect(await findByLabelText("select all job sets")).toBeInTheDocument()

    await userEvent.click(getByLabelText("Active only"))

    expect(await findByText("No job sets found for this queue.")).toBeInTheDocument()
  })

  it("surfaces a fetch failure as a snackbar without an unhandled rejection", async () => {
    const unhandledRejection = vi.fn()
    window.addEventListener("unhandledrejection", unhandledRejection)

    mockServer.use(http.post("/api/v1/jobGroups", () => HttpResponse.error()))

    const { findByText } = renderComponent("?queue=queue-1")

    expect(await findByText(/Failed to load job sets for queue queue-1/)).toBeInTheDocument()
    expect(unhandledRejection).not.toHaveBeenCalled()

    window.removeEventListener("unhandledrejection", unhandledRejection)
  })
})
