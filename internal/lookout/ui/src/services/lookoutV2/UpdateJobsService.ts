import _ from "lodash"
import { Job, JobId } from "models/lookoutV2Models"
import { SubmitApi } from "openapi/armada"
import { getErrorMessage } from "utils"

export type UpdateJobsResponse = {
  successfulJobIds: JobId[]
  failedJobIds: {
    jobId: JobId
    errorReason: string
  }[]
}

export class UpdateJobsService {
  constructor(private submitApi: SubmitApi) {}

  cancelJobs = async (jobs: Job[], reason: string): Promise<UpdateJobsResponse> => {
    const response: UpdateJobsResponse = { successfulJobIds: [], failedJobIds: [] }

    const maxJobsPerRequest = 10000
    const chunks = createJobBatches(jobs, maxJobsPerRequest)

    // Start all requests to allow them to fire off in parallel
    const apiResponsePromises = []
    for (const [queue, jobSetMap] of chunks) {
      for (const [jobSet, batches] of jobSetMap) {
        for (const batch of batches) {
          apiResponsePromises.push({
            promise: this.submitApi.cancelJobs({
              body: {
                jobIds: batch,
                queue: queue,
                jobSetId: jobSet,
                reason: reason,
              },
            }),
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
        console.error(e)
        const text = await getErrorMessage(e)
        const failedIds = apiResponsePromise.jobIds.map((jobId) => ({
          jobId: jobId,
          errorReason: text,
        }))
        response.failedJobIds.push(...failedIds)
      }
    }

    return response
  }

  reprioritiseJobs = async (jobs: Job[], newPriority: number): Promise<UpdateJobsResponse> => {
    const response: UpdateJobsResponse = { successfulJobIds: [], failedJobIds: [] }

    const maxJobsPerRequest = 10000
    const chunks = createJobBatches(jobs, maxJobsPerRequest)

    // Start all requests to allow them to fire off in parallel
    const apiResponsePromises = []
    for (const [queue, jobSetMap] of chunks) {
      for (const [jobSet, batches] of jobSetMap) {
        for (const batch of batches) {
          apiResponsePromises.push({
            promise: this.submitApi.reprioritizeJobs({
              body: {
                jobIds: batch,
                queue: queue,
                jobSetId: jobSet,
                newPriority: newPriority,
              },
            }),
            jobIds: batch,
          })
        }
      }
    }

    // Wait for all the responses
    for (const apiResponsePromise of apiResponsePromises) {
      try {
        const apiResponse = (await apiResponsePromise.promise)?.reprioritizationResults

        if (_.isNil(apiResponse)) {
          const errorMessage = "No reprioritization results found in response body"
          console.error(errorMessage)
          for (const jobId of apiResponsePromise.jobIds) {
            response.failedJobIds.push({ jobId: jobId, errorReason: errorMessage })
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
        console.error(e)
        const text = await getErrorMessage(e)
        apiResponsePromise.jobIds.forEach((jobId) => response.failedJobIds.push({ jobId, errorReason: text }))
      }
    }

    return response
  }
}

export function createJobBatches(jobs: Job[], batchSize: number): Map<string, Map<string, JobId[][]>> {
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
