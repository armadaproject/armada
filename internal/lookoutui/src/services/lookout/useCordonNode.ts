import { useMutation } from "@tanstack/react-query"

import { getConfig } from "../../config"
import { getErrorMessage } from "../../utils"
import { useApiClients } from "../apiClients"

export interface CordonNodeVariables {
  cluster: string
  node: string
}

export const useCordonNode = () => {
  const config = getConfig()
  const { getBinocularsApi } = useApiClients()

  return useMutation<object, string, CordonNodeVariables>({
    mutationFn: async ({ node, cluster }: CordonNodeVariables) => {
      if (config.fakeDataEnabled) {
        await new Promise((r) => setTimeout(r, 1_000))
        console.log(`Cordoned node ${node} in cluster ${cluster}`)
        return {}
      }

      try {
        return await getBinocularsApi(cluster).cordon({
          body: {
            nodeName: node,
          },
        })
      } catch (e) {
        throw await getErrorMessage(e)
      }
    },
  })
}
