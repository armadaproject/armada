import { ReactNode } from "react"

import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { act, cleanup, renderHook, waitFor } from "@testing-library/react"
import { http, HttpResponse } from "msw"
import { setupServer } from "msw/node"

import { makeTestJob } from "../../common/fakeJobsUtils"
import { Job, JobState } from "../../models/lookoutModels"
import { SubmitApi } from "../../openapi/armada"
import { FAKE_ARMADA_API_BASE_URL } from "../../setupTests"

import { ApiClientsProvider } from "../apiClients"

import { useCancelJobs } from "./useCancelJobs"
import { usePreemptJobs } from "./usePreemptJobs"
import { useReprioritizeJobs } from "./useReprioritizeJobs"

const actions = [
  {
    name: "cancel" as const,
    method: "cancelJobs" as const,
    resultField: "cancelledIds" as const,
    missingResultError: "failed to cancel job",
    fields: { reason: "cancel reason" },
    useAction: () => {
      const { mutateAsync } = useCancelJobs()
      return (jobs: Job[]) => mutateAsync({ jobs, reason: "cancel reason" })
    },
    success: (jobIds: string[]) => ({ cancelledIds: jobIds }),
  },
  {
    name: "preempt" as const,
    method: "preemptJobs" as const,
    resultField: "preemptionResults" as const,
    missingResultError: "No preemption results found in response body",
    fields: { reason: "preempt reason" },
    useAction: () => {
      const { mutateAsync } = usePreemptJobs()
      return (jobs: Job[]) => mutateAsync({ jobs, preemptReason: "preempt reason" })
    },
    success: (jobIds: string[]) => ({ preemptionResults: Object.fromEntries(jobIds.map((id) => [id, ""])) }),
  },
  {
    name: "reprioritize" as const,
    method: "reprioritizeJobs" as const,
    resultField: "reprioritizationResults" as const,
    missingResultError: "No reprioritization results found in response body",
    fields: { newPriority: 7 },
    useAction: () => {
      const { mutateAsync } = useReprioritizeJobs()
      return (jobs: Job[], newPriority = 7) => mutateAsync({ jobs, newPriority })
    },
    success: (jobIds: string[]) => ({ reprioritizationResults: Object.fromEntries(jobIds.map((id) => [id, ""])) }),
  },
]

type Action = (typeof actions)[number]
type ApiResult = Awaited<ReturnType<SubmitApi["cancelJobs"]>> &
  Awaited<ReturnType<SubmitApi["preemptJobs"]>> &
  Awaited<ReturnType<SubmitApi["reprioritizeJobs"]>>

const server = setupServer()
let client: QueryClient
let requests: { url: string; method: string; body: unknown }[]

const wrapper = ({ children }: { children: ReactNode }) => (
  <QueryClientProvider client={client}>
    <ApiClientsProvider>{children}</ApiClientsProvider>
  </QueryClientProvider>
)

const job = (jobId: string, queue = "queue", jobSet = "set") => makeTestJob(queue, jobSet, jobId, JobState.Running)
const failures = (ids: string[], errorReason: string) => ids.map((jobId) => ({ jobId, errorReason }))

function respond(action: Action, body?: object, status = 200) {
  server.use(
    http.post(`${FAKE_ARMADA_API_BASE_URL}/v1/job/${action.name}`, async ({ request }) => {
      const requestBody = (await request.json()) as { jobIds: string[] }
      requests.push({ url: request.url, method: request.method, body: requestBody })
      return HttpResponse.json(body ?? action.success(requestBody.jobIds), { status })
    }),
  )
}

function expectedRequest(action: Action, queue: string, jobSetId: string, jobIds: string[]) {
  return {
    url: `${FAKE_ARMADA_API_BASE_URL}/v1/job/${action.name}`,
    method: "POST",
    body: { queue, jobSetId, jobIds, ...action.fields },
  }
}

function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (reason: unknown) => void
  const promise = new Promise<T>((res, rej) => {
    resolve = res
    reject = rej
  })
  return { promise, resolve, reject }
}

beforeAll(() => server.listen({ onUnhandledRequest: "error" }))
beforeEach(() => {
  client = new QueryClient({ defaultOptions: { mutations: { retry: false } } })
  requests = []
})
afterEach(() => {
  cleanup()
  client.clear()
  server.resetHandlers()
  vi.restoreAllMocks()
  vi.useRealTimers()
  window.history.replaceState(null, "", "/")
})
afterAll(() => server.close())

describe.each(actions)("$name job requests", (action) => {
  it("groups by queue then job set in first-seen order, retaining duplicate IDs without changing input", async () => {
    respond(action)
    const jobs = [
      job("z", "queue-b", "set-2"),
      job("a", "queue-a", "set-1"),
      job("b", "queue-b", "set-1"),
      job("c", "queue-b", "set-2"),
      job("z", "queue-b", "set-2"),
      job("z", "queue-a", "set-2"),
      job("b", "queue-b", "set-1"),
    ]
    const original = structuredClone(jobs)
    jobs.forEach(Object.freeze)
    Object.freeze(jobs)
    const { result } = renderHook(action.useAction, { wrapper })

    await act(async () => {
      expect(await result.current(jobs)).toEqual({
        successfulJobIds: ["z", "c", "z", "b", "b", "a", "z"],
        failedJobIds: [],
      })
    })

    expect(requests).toEqual([
      expectedRequest(action, "queue-b", "set-2", ["z", "c", "z"]),
      expectedRequest(action, "queue-b", "set-1", ["b", "b"]),
      expectedRequest(action, "queue-a", "set-1", ["a"]),
      expectedRequest(action, "queue-a", "set-2", ["z"]),
    ])
    expect(jobs).toEqual(original)
  })

  it.each([10000, 10001])("batches %i IDs per group, with no empty trailing request", async (count) => {
    respond(action)
    const ids = Array.from({ length: count }, (_, i) => `job-${count - i}`)
    // The same ID occurs on both sides of the boundary in the 10,001 case.
    ids[count - 1] = ids[0]
    const jobs = [job("other-queue", "queue-a"), ...ids.map((id) => job(id)), job("other-set", "queue", "set-2")]
    const original = structuredClone(jobs)
    jobs.forEach(Object.freeze)
    Object.freeze(jobs)
    const { result } = renderHook(action.useAction, { wrapper })

    await act(async () => {
      expect(await result.current(jobs)).toEqual({
        successfulJobIds: ["other-queue", ...ids, "other-set"],
        failedJobIds: [],
      })
    })

    expect(requests).toEqual([
      expectedRequest(action, "queue-a", "set", ["other-queue"]),
      expectedRequest(action, "queue", "set", ids.slice(0, 10000)),
      ...(count === 10001 ? [expectedRequest(action, "queue", "set", ids.slice(10000))] : []),
      expectedRequest(action, "queue", "set-2", ["other-set"]),
    ])
    expect(jobs).toEqual(original)
  })

  it("returns empty results without a request for empty input", async () => {
    respond(action)
    const { result } = renderHook(action.useAction, { wrapper })
    await act(async () => {
      expect(await result.current([])).toEqual({ successfulJobIds: [], failedJobIds: [] })
    })
    expect(requests).toEqual([])
  })

  it.each([undefined, null])("fails all requested IDs for a missing/null result (%s)", async (value) => {
    respond(action, value === undefined ? {} : { [action.resultField]: value })
    const { result } = renderHook(action.useAction, { wrapper })
    await act(async () => {
      expect(await result.current([job("z"), job("a"), job("z")])).toEqual({
        successfulJobIds: [],
        failedJobIds: failures(["z", "a", "z"], action.missingResultError),
      })
    })
  })

  it("converts an HTTP rejection using its response message", async () => {
    respond(action, { message: "queue permission denied" }, 403)
    const { result } = renderHook(action.useAction, { wrapper })
    await act(async () => {
      expect(await result.current([job("z"), job("a"), job("z")])).toEqual({
        successfulJobIds: [],
        failedJobIds: failures(["z", "a", "z"], "queue permission denied"),
      })
    })
  })

  it("launches every request before completion and consumes responses in launch order", async () => {
    const first = deferred<ApiResult>()
    const second = deferred<ApiResult>()
    const third = deferred<ApiResult>()
    const fourth = deferred<ApiResult>()
    // Handle controlled rejections even if an assertion stops the test before the hook awaits them.
    // The hook still receives the original promise and converts its rejection itself.
    void second.promise.catch(() => undefined)
    const api = vi
      .spyOn(SubmitApi.prototype, action.method)
      .mockReturnValueOnce(first.promise)
      .mockReturnValueOnce(second.promise)
      .mockReturnValueOnce(third.promise)
      .mockReturnValueOnce(fourth.promise)
    const jobs = [
      job("first", "queue-b"),
      job("bad", "queue-b", "set-2"),
      job("bad", "queue-b", "set-2"),
      job("third", "queue-a"),
      job("fourth", "queue-a", "set-2"),
    ]
    const consumed: string[] = []
    const response = (id: string, success = true): ApiResult => ({
      get [action.resultField]() {
        consumed.push(id)
        return success ? Object.values(action.success([id]))[0] : undefined
      },
    })
    const { result } = renderHook(action.useAction, { wrapper })
    const settled = vi.fn()
    const pending = result.current(jobs).then((value) => {
      settled()
      return value
    })

    await waitFor(() => expect(api).toHaveBeenCalledTimes(4))
    expect(api.mock.calls.map(([request]) => request.body)).toEqual([
      { queue: "queue-b", jobSetId: "set", jobIds: ["first"], ...action.fields },
      { queue: "queue-b", jobSetId: "set-2", jobIds: ["bad", "bad"], ...action.fields },
      { queue: "queue-a", jobSetId: "set", jobIds: ["third"], ...action.fields },
      { queue: "queue-a", jobSetId: "set-2", jobIds: ["fourth"], ...action.fields },
    ])
    await act(async () => {
      fourth.resolve(response("fourth", false))
      third.resolve(response("third"))
    })
    expect(consumed).toEqual([])
    expect(settled).not.toHaveBeenCalled()

    await act(async () => first.resolve(response("first")))
    await waitFor(() => expect(consumed).toContain("first"))
    expect(consumed).not.toContain("third")
    expect(settled).not.toHaveBeenCalled()

    await act(async () => {
      second.reject(new Error("request failed"))
      expect(await pending).toEqual({
        successfulJobIds: ["first", "third"],
        failedJobIds: [
          ...failures(["bad", "bad"], "Error: request failed"),
          ...failures(["fourth"], action.missingResultError),
        ],
      })
    })
    // Cancel reads its list twice; compare transitions rather than property read counts.
    expect([...new Set(consumed)]).toEqual(["first", "third", "fourth"])
  })

  it("rejects the mutation on a synchronous API throw and stops launching later requests", async () => {
    const api = vi
      .spyOn(SubmitApi.prototype, action.method)
      .mockResolvedValueOnce(action.success(["first"]))
      .mockImplementationOnce(() => {
        throw new Error("synchronous failure")
      })
    const { result } = renderHook(action.useAction, { wrapper })
    await act(async () => {
      await expect(result.current([job("first"), job("second", "queue-2"), job("third", "queue-3")])).rejects.toBe(
        "Error: synchronous failure",
      )
    })
    expect(api).toHaveBeenCalledTimes(2)
  })
})

it("cancel preserves server list order, duplicates and unexpected IDs, and duplicate failures", async () => {
  respond(actions[0], { cancelledIds: ["a", "unexpected", "z", "a"] })
  const { result } = renderHook(actions[0].useAction, { wrapper })
  await act(async () => {
    expect(await result.current([job("z"), job("missing"), job("a"), job("z"), job("missing")])).toEqual({
      successfulJobIds: ["a", "unexpected", "z", "a"],
      failedJobIds: failures(["missing", "missing"], "failed to cancel job"),
    })
  })
})

describe.each(actions.slice(1))("$name result maps", (action) => {
  it("uses requested ID order, preserving duplicate successes/errors and ignoring extra keys", async () => {
    respond(action, { [action.resultField]: { a: "", bad: "exact per-job error", z: "", extra: "ignored error" } })
    const { result } = renderHook(action.useAction, { wrapper })
    await act(async () => {
      expect(await result.current([job("z"), job("bad"), job("absent"), job("a"), job("z"), job("bad")])).toEqual({
        successfulJobIds: ["z", "absent", "a", "z"],
        failedJobIds: failures(["bad", "bad"], "exact per-job error"),
      })
    })
  })

  it("treats a present empty map as success for every requested ID", async () => {
    respond(action, { [action.resultField]: {} })
    const { result } = renderHook(action.useAction, { wrapper })
    await act(async () => {
      expect(await result.current([job("z"), job("a"), job("z")])).toEqual({
        successfulJobIds: ["z", "a", "z"],
        failedJobIds: [],
      })
    })
  })
})

it("sends zero as a numeric newPriority without reason fields", async () => {
  const action = actions[2]
  respond(action)
  const { result } = renderHook(action.useAction, { wrapper })
  await act(async () => {
    await result.current([job("zero")], 0)
  })
  expect(requests).toEqual([
    {
      url: `${FAKE_ARMADA_API_BASE_URL}/v1/job/reprioritize`,
      method: "POST",
      body: { queue: "queue", jobSetId: "set", jobIds: ["zero"], newPriority: 0 },
    },
  ])
})

describe.each(actions)("$name configuration timing", (action) => {
  it.each([false, true])("captures fake-data mode at the original location (enabled: %s)", async (fakeData) => {
    vi.resetModules()
    // Cancel captures config when rendered; the other two capture it when imported.
    window.history.replaceState(null, "", fakeData ? "/?fakeData" : "/")
    const hooks = {
      cancel: (await import("./useCancelJobs")).useCancelJobs,
      preempt: (await import("./usePreemptJobs")).usePreemptJobs,
      reprioritize: (await import("./useReprioritizeJobs")).useReprioritizeJobs,
    }
    window.history.replaceState(null, "", fakeData ? "/" : "/?fakeData")
    const { ApiClientsProvider: FreshApiClientsProvider } = await import("../apiClients")
    const { SubmitApi: FreshSubmitApi } = await import("../../openapi/armada")
    const useAction = hooks[action.name]
    const { result } = renderHook(() => useAction(), {
      wrapper: ({ children }: { children: ReactNode }) => (
        <QueryClientProvider client={client}>
          <FreshApiClientsProvider>{children}</FreshApiClientsProvider>
        </QueryClientProvider>
      ),
    })
    // Changing the URL after rendering must not change the captured configuration.
    window.history.replaceState(null, "", fakeData ? "/?fakeData" : "/")
    const usesFakeData = action.name === "cancel" ? !fakeData : fakeData
    const api = vi.spyOn(FreshSubmitApi.prototype, action.method).mockResolvedValue(action.success(["real"]))
    const jobs = [job("z", "queue-b"), job("a", "queue-a"), job("z", "queue-b")]
    vi.useFakeTimers()
    const settled = vi.fn()
    const pending = result.current
      .mutateAsync({ jobs, reason: "reason", preemptReason: "reason", newPriority: 0 })
      .then((value) => {
        settled()
        return value
      })

    await act(async () => {
      await vi.advanceTimersByTimeAsync(999)
    })
    if (usesFakeData) {
      expect(api).not.toHaveBeenCalled()
      expect(settled).not.toHaveBeenCalled()
      await act(async () => {
        await vi.advanceTimersByTimeAsync(1)
        expect(await pending).toEqual({ successfulJobIds: ["z", "a", "z"], failedJobIds: [] })
      })
      expect(api).not.toHaveBeenCalled()
    } else {
      expect(api).toHaveBeenCalledTimes(2)
      expect(settled).toHaveBeenCalledOnce()
      await pending
    }
  })
})
