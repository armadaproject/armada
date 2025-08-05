import { http, HttpResponse } from "msw"
import { setupServer, SetupServerApi } from "msw/node"

import { FAKE_ARMADA_API_BASE_URL } from "../../../setupTests"

const GET_QUEUES_ENDPOINT = `${FAKE_ARMADA_API_BASE_URL}/v1/batched/queues`
const POST_JOB_RUN_ERROR_ENDPOINT = "/api/v1/jobRunError"
const POST_JOB_RUN_DEBUG_MESSAGE_ENDPOINT = "/api/v1/jobRunDebugMessage"

export class MockServer {
  private server: SetupServerApi

  constructor() {
    this.server = setupServer()
  }

  listen() {
    return this.server.listen({ onUnhandledRequest: "error" })
  }

  reset() {
    return this.server.restoreHandlers()
  }

  close() {
    return this.server.close()
  }

  setGetQueuesResponse(queueNames: string[]) {
    this.server.use(
      http.get(GET_QUEUES_ENDPOINT, () =>
        HttpResponse.text(
          queueNames
            .map((name) =>
              JSON.stringify({
                result: { Event: { queue: { name } } },
              }),
            )
            .join("\n"),
        ),
      ),
    )
  }

  setPostJobRunErrorResponseForRunId(runId: string, errorMessage: string) {
    this.server.use(
      http.post<object, { runId: string }, { errorMessage: string }>(POST_JOB_RUN_ERROR_ENDPOINT, async (req) => {
        const reqJson = await req.request.json()
        if (reqJson.runId === runId) {
          return HttpResponse.json({ errorMessage })
        }
      }),
    )
  }

  setPostJobRunDebugMessageResponseForRunId(runId: string, errorMessage: string) {
    this.server.use(
      http.post<object, { runId: string }, { errorMessage: string }>(
        POST_JOB_RUN_DEBUG_MESSAGE_ENDPOINT,
        async (req) => {
          const reqJson = await req.request.json()
          if (reqJson.runId === runId) {
            return HttpResponse.json({ errorMessage })
          }
        },
      ),
    )
  }
}
