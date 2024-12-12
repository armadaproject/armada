import { InfiniteData, useInfiniteQuery } from "@tanstack/react-query"

import { LogLine } from "./LogService"
import { getAccessToken, useUserManager } from "../../oidc"
import { getErrorMessage } from "../../utils"
import { useServices } from "../context"

const INITIAL_TAIL_LINES = 1000

export const useGetLogs = (
  cluster: string,
  namespace: string,
  jobId: string,
  container: string,
  loadFromStart: boolean,
  enabled = true,
) => {
  const { v2LogService } = useServices()
  const userManager = useUserManager()

  return useInfiniteQuery<
    LogLine[],
    string,
    InfiniteData<LogLine[]>,
    ["getLogs", string, string, string, string, boolean],
    string
  >({
    queryKey: ["getLogs", cluster, namespace, jobId, container, loadFromStart],
    queryFn: async ({ pageParam, signal }) => {
      try {
        const accessToken = userManager && (await getAccessToken(userManager))
        const logLines = await v2LogService.getLogs(
          cluster,
          namespace,
          jobId,
          container,
          pageParam,
          loadFromStart ? undefined : INITIAL_TAIL_LINES,
          accessToken,
          signal,
        )

        // Remove log lines with the same timestamp as the previous since-time (pageParam)
        let sliceIndex = 0
        for (let i = 0; i < logLines.length; i++) {
          if (logLines[i].timestamp > pageParam) {
            break
          }
          sliceIndex = i + 1
        }

        return logLines.slice(sliceIndex)
      } catch (e) {
        throw await getErrorMessage(e)
      }
    },
    initialPageParam: "",
    getNextPageParam: (_, allPages) => allPages.flat().at(-1)?.timestamp ?? "",
    enabled,
  })
}
