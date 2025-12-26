import { useQuery } from "@tanstack/react-query"
import { isNil } from "lodash"

import { getErrorMessage } from "../../common/utils"
import { getConfig } from "../../config"
import { useAuthenticatedFetch } from "../../oidcAuth"
import { ApiQueue, ApiQueueFromJSON } from "../../openapi/armada"

export const useGetQueues = (enabled = true) => {
  const config = getConfig()

  const armadaApiBaseUrl = config.armadaApiBaseUrl

  const authenticatedFetch = useAuthenticatedFetch()

  return useQuery<ApiQueue[], string>({
    queryKey: ["getQueues"],
    queryFn: async ({ signal }) => {
      try {
        const response = await authenticatedFetch(`${armadaApiBaseUrl}/v1/batched/queues`, {
          method: "GET",
          signal,
        })
        if (response.status < 200 || response.status >= 300) {
          throw response
        }

        const responseText = await response.text()
        return responseText
          .trim()
          .split("\n")
          .map((text) => JSON.parse(text))
          .flatMap((json) => {
            if (json.error && !isNil(json.error)) {
              throw json["error"]
            }

            if (json.result?.Event?.queue) {
              return [ApiQueueFromJSON(json.result.Event.queue)]
            }

            return []
          })
      } catch (e) {
        throw await getErrorMessage(e)
      }
    },
    enabled: Boolean(enabled && armadaApiBaseUrl),
  })
}
