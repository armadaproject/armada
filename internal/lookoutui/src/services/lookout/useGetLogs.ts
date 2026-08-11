import { useCallback } from "react"

import { InfiniteData, useInfiniteQuery, useQueryClient } from "@tanstack/react-query"

import { getErrorMessage } from "../../common/utils"
import { getConfig } from "../../config"

import { useApiClients } from "../apiClients"

import { createFakeLogs } from "./mocks/fakeData"

const INITIAL_TAIL_LINES = 1000

export type LogLine = {
  timestamp: string
  line: string
}

const fetchLogsFromStartPage = async (
  getBinocularsApi: ReturnType<typeof useApiClients>["getBinocularsApi"],
  cluster: string,
  namespace: string,
  jobId: string,
  container: string,
  sinceTime: string,
): Promise<LogLine[]> => {
  const config = getConfig()
  try {
    const logLinesRaw = config.fakeDataEnabled
      ? createFakeLogs(cluster, namespace, jobId, container, sinceTime)
      : (
          await getBinocularsApi(cluster).logs({
            body: {
              jobId,
              podNumber: 0,
              podNamespace: namespace,
              sinceTime,
              logOptions: {
                container: container,
                tailLines: undefined,
              },
            },
          })
        ).log

    const logLines = (logLinesRaw ?? []).map((l) => ({
      timestamp: l.timestamp ?? "",
      line: l.line ?? "",
    }))

    let sliceIndex = 0
    for (let i = 0; i < logLines.length; i++) {
      if (logLines[i].timestamp > sinceTime) {
        break
      }
      sliceIndex = i + 1
    }

    return logLines.slice(sliceIndex)
  } catch (e) {
    throw await getErrorMessage(e)
  }
}

export const useGetLogs = (
  cluster: string,
  namespace: string,
  jobId: string,
  container: string,
  loadFromStart: boolean,
  enabled = true,
) => {
  const config = getConfig()
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
        const logLinesRaw = config.fakeDataEnabled
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

export const useFetchAllLogsFromStart = () => {
  const queryClient = useQueryClient()
  const { getBinocularsApi } = useApiClients()

  return useCallback(
    async (cluster: string, namespace: string, jobId: string, container: string): Promise<LogLine[]> => {
      const allLogLines: LogLine[] = []
      let sinceTime = ""
      for (;;) {
        const page = await queryClient.fetchQuery({
          // eslint-disable-next-line @tanstack/query/exhaustive-deps -- getBinocularsApi is a stable, memoised reference and does not vary the fetched data
          queryKey: ["getLogsFromStartPage", cluster, namespace, jobId, container, sinceTime],
          queryFn: () => fetchLogsFromStartPage(getBinocularsApi, cluster, namespace, jobId, container, sinceTime),
        })
        if (page.length === 0) {
          break
        }
        allLogLines.push(...page)
        sinceTime = page[page.length - 1].timestamp
      }
      return allLogLines
    },
    [queryClient, getBinocularsApi],
  )
}
