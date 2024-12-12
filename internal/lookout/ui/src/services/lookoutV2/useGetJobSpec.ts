import { useQuery } from "@tanstack/react-query"

import { getErrorMessage } from "../../utils"
import { useServices } from "../context"

export const useGetJobSpec = (jobId: string, enabled = true) => {
  const { v2JobSpecService } = useServices()

  return useQuery<Record<string, any>, string>({
    queryKey: ["getJobSpec", jobId],
    queryFn: async ({ signal }) => {
      try {
        return await v2JobSpecService.getJobSpec(jobId, signal)
      } catch (e) {
        throw await getErrorMessage(e)
      }
    },
    enabled,
  })
}
