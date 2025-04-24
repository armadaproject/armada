import { InfiniteData, useInfiniteQuery } from "@tanstack/react-query"

import { getErrorMessage } from "../../utils"
import { useApiClients } from "../apiClients"
import { createFakeLogs } from "./mocks/fakeData"
import { useGetUiConfig } from "./useGetUiConfig"

const INITIAL_TAIL_LINES = 1000

export type LogLine = {
  timestamp: string
  line: string
}

export const useGetLogs = (
  cluster: string,
  namespace: string,
  jobId: string,
  container: string,
  loadFromStart: boolean,
  enabled = true,
) => {
  const { data: uiConfig } = useGetUiConfig()
  const { getBinocularsApi } = useApiClients()

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
        const logLinesRaw = uiConfig?.fakeDataEnabled
          ? createFakeLogs(cluster, namespace, jobId, container, pageParam)
          : (
              await getBinocularsApi(cluster).logs(
                {
                  body: {
                    jobId,
                    podNumber: 0,
                    podNamespace: namespace,
                    sinceTime: pageParam,
                    logOptions: {
                      container: container,
                      tailLines: loadFromStart ? undefined : INITIAL_TAIL_LINES,
                    },
                  },
                },
                { signal },
              )
            ).log

        const logLines = (logLinesRaw ?? []).map((l) => ({
          timestamp: l.timestamp ?? "",
          line: l.line ?? "",
        }))

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
