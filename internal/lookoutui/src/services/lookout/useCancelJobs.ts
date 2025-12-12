import { useMutation } from "@tanstack/react-query"

import { getErrorMessage } from "../../common/utils"
import { getConfig } from "../../config"
import { Job, JobId } from "../../models/lookoutModels"
import { appendAuthorizationHeaders, useGetAccessToken } from "../../oidcAuth"

import { useApiClients } from "../apiClients"

export type UpdateJobsResponse = {
  successfulJobIds: JobId[]
  failedJobIds: {
    jobId: JobId
    errorReason: string
  }[]
}

export interface CancelJobsVariables {
  jobs: Job[]
  reason: string
}

function createJobBatches(jobs: Job[], batchSize: number): Map<string, Map<string, JobId[][]>> {
  const result = new Map<string, Map<string, JobId[][]>>()
  for (const job of jobs) {
    if (!result.has(job.queue)) {
      result.set(job.queue, new Map<string, JobId[][]>())
    }
    if (!result.get(job.queue)?.has(job.jobSet)) {
      result.get(job.queue)?.set(job.jobSet, [])
    }

    const batches = result.get(job.queue)?.get(job.jobSet) ?? []
    if (batches.length === 0 || batches[batches.length - 1].length === batchSize) {
      batches.push([job.jobId])
      continue
    }

    const lastBatch = batches[batches.length - 1]
    lastBatch.push(job.jobId)
  }
  return result
}

export const useCancelJobs = () => {
  const config = getConfig()
  const { submitApi } = useApiClients()
  const getAccessToken = useGetAccessToken()

  return useMutation<UpdateJobsResponse, string, CancelJobsVariables>({
    mutationFn: async ({ jobs, reason }: CancelJobsVariables) => {
      if (config.fakeDataEnabled) {
        await new Promise((r) => setTimeout(r, 1_000))
        return {
          successfulJobIds: jobs.map((job) => job.jobId),
          failedJobIds: [],
        }
      }

      try {
        const response: UpdateJobsResponse = { successfulJobIds: [], failedJobIds: [] }

        const maxJobsPerRequest = 10000
        const chunks = createJobBatches(jobs, maxJobsPerRequest)

        const accessToken = await getAccessToken()

        const apiResponsePromises = []
        for (const [queue, jobSetMap] of chunks) {
          for (const [jobSet, batches] of jobSetMap) {
            for (const batch of batches) {
              const headers = new Headers()
              if (accessToken) {
                appendAuthorizationHeaders(headers, accessToken)
              }
              apiResponsePromises.push({
                promise: submitApi.cancelJobs(
                  {
                    body: {
                      jobIds: batch,
                      queue: queue,
                      jobSetId: jobSet,
                      reason: reason,
                    },
                  },
                  { headers },
                ),
                jobIds: batch,
              })
            }
          }
        }

        for (const apiResponsePromise of apiResponsePromises) {
          try {
            const result = await apiResponsePromise.promise
            const cancelledIds = result.cancelledIds ?? []
            const cancelledIdsSet = new Set<string>(result.cancelledIds)
            const failedIds = apiResponsePromise.jobIds
              .filter((jobId) => !cancelledIdsSet.has(jobId))
              .map((jobId) => ({
                jobId: jobId,
                errorReason: "failed to cancel job",
              }))
            response.successfulJobIds.push(...cancelledIds)
            response.failedJobIds.push(...failedIds)
          } catch (e) {
            const text = await getErrorMessage(e)
            const failedIds = apiResponsePromise.jobIds.map((jobId) => ({
              jobId: jobId,
              errorReason: text,
            }))
            response.failedJobIds.push(...failedIds)
          }
        }

        return response
      } catch (e) {
        throw await getErrorMessage(e)
      }
    },
  })
}
