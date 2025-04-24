import { useMutation } from "@tanstack/react-query"

import { getErrorMessage } from "../../utils"
import { useApiClients } from "../apiClients"
import { useGetUiConfig } from "./useGetUiConfig"

export interface CordonNodeVariables {
  cluster: string
  node: string
}

export const useCordonNode = () => {
  const { data: uiConfig } = useGetUiConfig()
  const { getBinocularsApi } = useApiClients()

  return useMutation<object, string, CordonNodeVariables>({
    mutationFn: async ({ node, cluster }: CordonNodeVariables) => {
      if (uiConfig?.fakeDataEnabled) {
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
