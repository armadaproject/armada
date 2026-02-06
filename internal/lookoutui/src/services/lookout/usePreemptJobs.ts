import { useMutation } from "@tanstack/react-query"
import _ from "lodash"

import { getErrorMessage } from "../../common/utils"
import { getConfig } from "../../config"
import { Job, JobId } from "../../models/lookoutModels"

import { useApiClients } from "../apiClients"

const config = getConfig()
const maxJobsPerRequest = 10000

export type UpdateJobsResponse = {
  status_code: number
  status_text?: string
  successfulJobIds: JobId[]
  failedJobIds: {
    jobId: JobId
    errorReason: string
  }[]
}

export interface PreemptJobsVariables {
  jobs: Job[]
  preemptReason: string
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

export const usePreemptJobs = () => {
  const { submitApi } = useApiClients()

  return useMutation<UpdateJobsResponse, string, PreemptJobsVariables>({
    mutationFn: async ({ jobs, preemptReason }: PreemptJobsVariables) => {
      if (config.fakeDataEnabled) {
        await new Promise((r) => setTimeout(r, 1_000))
        return {
          status_code: 200,
          successfulJobIds: jobs.map((job) => job.jobId),
          failedJobIds: [],
        }
      }

      try {
        const response: UpdateJobsResponse = {
          status_code: 200,
          successfulJobIds: jobs.map((job) => job.jobId),
          failedJobIds: [],
        }

        const chunks = createJobBatches(jobs, maxJobsPerRequest)

        const apiResponsePromises = []
        for (const [queue, jobSets] of chunks) {
          for (const [jobSet, batches] of jobSets) {
            for (const batch of batches) {
              apiResponsePromises.push({
                promise: submitApi.preemptJobsRaw({
                  // Use raw to access status code
                  body: {
                    jobIds: batch,
                    queue: queue,
                    jobSetId: jobSet,
                    reason: preemptReason,
                  },
                }),
                jobIds: batch,
              })
            }
          }
        }

        for (const apiResponsePromise of apiResponsePromises) {
          try {
            const apiResponse = await apiResponsePromise.promise
            const statusCode = apiResponse.raw.status
            const statusText = apiResponse.raw.statusText

            // If any request fails, capture the error for each job in the batch
            if (statusCode < 200 || statusCode >= 300) {
              response.status_code = statusCode
              response.status_text = statusText

              // Mark all jobs in this batch as failed
              for (const jobId of apiResponsePromise.jobIds) {
                response.failedJobIds.push({
                  jobId: jobId,
                  errorReason: statusText || `Request failed with status ${statusCode}`,
                })
              }
              return response
            } else {
              // Otherwise, all jobs in this batch are successful
              response.successfulJobIds.push(...apiResponsePromise.jobIds)
            }
          } catch (e) {
            const text = await getErrorMessage(e)
            response.status_code = 500
            response.status_text = text

            for (const jobId of apiResponsePromise.jobIds) {
              response.failedJobIds.push({
                jobId: jobId,
                errorReason: text,
              })
            }
            return response
          }
        }

        return response
      } catch (e) {
        throw await getErrorMessage(e)
      }
    },
  })
}
