import { useMutation } from "@tanstack/react-query"
import _ from "lodash"

import { getErrorMessage } from "../../common/utils"
import { getConfig } from "../../config"
import { Job, JobId } from "../../models/lookoutModels"

import { useApiClients } from "../apiClients"

const config = getConfig()
const maxJobsPerRequest = 10000

export type UpdateJobsResponse = {
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
          successfulJobIds: jobs.map((job) => job.jobId),
          failedJobIds: [],
        }
      }

      try {
        const response: UpdateJobsResponse = { successfulJobIds: [], failedJobIds: [] }

        const chunks = createJobBatches(jobs, maxJobsPerRequest)

        const apiResponsePromises = []
        for (const [queue, jobSets] of chunks) {
          for (const [jobSet, batches] of jobSets) {
            for (const batch of batches) {
              apiResponsePromises.push({
                promise: submitApi.preemptJobs({
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
            const apiResponse = (await apiResponsePromise.promise)?.preemptionResults

            if (_.isNil(apiResponse)) {
              const errorMessage = "No preemption results found in response body"
              for (const jobId of apiResponsePromise.jobIds) {
                response.failedJobIds.push({ jobId, errorReason: errorMessage })
              }
            } else {
              for (const jobId of apiResponsePromise.jobIds) {
                if (jobId in apiResponse) {
                  const emptyOrError = apiResponse[jobId]
                  if (emptyOrError === "") {
                    response.successfulJobIds.push(jobId)
                  } else {
                    response.failedJobIds.push({ jobId, errorReason: emptyOrError })
                  }
                } else {
                  response.successfulJobIds.push(jobId)
                }
              }
            }
          } catch (e) {
            const text = await getErrorMessage(e)
            apiResponsePromise.jobIds.forEach((jobId) => response.failedJobIds.push({ jobId, errorReason: text }))
          }
        }

        return response
      } catch (e) {
        throw await getErrorMessage(e)
      }
    },
  })
}
