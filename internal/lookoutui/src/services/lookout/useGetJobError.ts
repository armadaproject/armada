import { useQuery } from "@tanstack/react-query"

import { getErrorMessage } from "../../common/utils"
import { getConfig } from "../../config"
import { useAuthenticatedFetch } from "../../oidcAuth"

import { fakeJobError } from "./mocks/fakeData"

export const useGetJobError = (jobId: string, enabled = true) => {
  const config = getConfig()

  const authenticatedFetch = useAuthenticatedFetch()

  return useQuery<string, string>({
    queryKey: ["getJobError", jobId],
    queryFn: async ({ signal }) => {
      try {
        if (config.fakeDataEnabled) {
          return fakeJobError
        }

        const response = await authenticatedFetch("/api/v1/jobError", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ jobId }),
          signal,
        })

        const json = await response.json()
        return json.errorString ?? ""
      } catch (e) {
        throw await getErrorMessage(e)
      }
    },
    enabled,
    refetchOnMount: false,
    staleTime: 30_000,
  })
}
