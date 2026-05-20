import { useQuery } from "@tanstack/react-query"

import { makeFakeJobSpec } from "../../common/fakeJobsUtils"
import { getErrorMessage } from "../../common/utils"
import { getConfig } from "../../config"
import { useAuthenticatedFetch } from "../../oidcAuth"

export const useGetJobSpec = (jobId: string, enabled = true) => {
  const authenticatedFetch = useAuthenticatedFetch()
  const config = getConfig()

  return useQuery<Record<string, any>, string>({
    queryKey: ["getJobSpec", jobId],
    queryFn: async ({ signal }) => {
      try {
        if (config.fakeDataEnabled) {
          return makeFakeJobSpec(jobId)
        }

        const response = await authenticatedFetch("/api/v1/jobSpec", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ jobId }),
          signal,
        })

        const json = await response.json()
        return json.job ?? {}
      } catch (e) {
        throw await getErrorMessage(e)
      }
    },
    enabled,
  })
}
