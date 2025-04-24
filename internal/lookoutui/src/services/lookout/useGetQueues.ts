import { useQuery } from "@tanstack/react-query"
import { isNil } from "lodash"

import { useGetUiConfig } from "./useGetUiConfig"
import { useAuthenticatedFetch } from "../../oidcAuth"
import { ApiQueue, ApiQueueFromJSON } from "../../openapi/armada"
import { getErrorMessage } from "../../utils"

export const useGetQueues = (enabled = true) => {
  const { data: uiConfig } = useGetUiConfig(enabled)

  const armadaApiBaseUrl = uiConfig?.armadaApiBaseUrl

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
