import { http, HttpResponse } from "msw"
import { setupServer, SetupServerApi } from "msw/node"

const FAKE_ARMADA_API_BASE_URL = "https://test-armada-api.aramada.com"
const GET_QUEUES_ENDPOINT = `${FAKE_ARMADA_API_BASE_URL}/v1/batched/queues`

const configHandler = http.get("/config", () =>
  HttpResponse.json({
    ArmadaApiBaseUrl: FAKE_ARMADA_API_BASE_URL,
  }),
)

export class MockServer {
  private server: SetupServerApi

  constructor() {
    this.server = setupServer(configHandler)
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
}
