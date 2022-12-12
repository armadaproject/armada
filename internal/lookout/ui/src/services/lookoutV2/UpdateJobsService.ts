import _ from "lodash"
import { JobId } from "models/lookoutV2Models"
import { SubmitApi } from "openapi/armada"
import { getErrorMessage } from "utils"

export interface UpdateJobsResponse {
  successfulJobIds: JobId[]
  failedJobIds: {
    jobId: JobId
    errorReason: string
  }[]
}

export class UpdateJobsService {
  constructor(private submitApi: SubmitApi) {}

  // TODO: Use a batch cancel-jobs API endpoint when available
  cancelJobs = async (jobIds: JobId[]): Promise<UpdateJobsResponse> => {
    const response: UpdateJobsResponse = { successfulJobIds: [], failedJobIds: [] }

    // Start all requests to allow them to fire off in parallel
    const apiResponsePromises = jobIds.map((jobId) => ({
      jobId,
      promise: this.submitApi.cancelJobs({
        body: {
          jobId: jobId,
        },
      }),
    }))

    // Wait for all the responses
    for (const apiResponsePromise of apiResponsePromises) {
      const { jobId, promise } = apiResponsePromise
      try {
        const apiResponse = await promise

        if (
          !apiResponse.cancelledIds ||
          apiResponse.cancelledIds.length !== 1 ||
          apiResponse.cancelledIds[0] !== jobId
        ) {
          response.failedJobIds.push({ jobId, errorReason: "No job was cancelled" })
        } else {
          response.successfulJobIds.push(jobId)
        }
      } catch (e) {
        console.error(e)
        const text = await getErrorMessage(e)
        response.failedJobIds.push({ jobId, errorReason: text })
      }
    }
    return response
  }

  reprioritiseJobs = async (jobIds: JobId[], newPriority: number): Promise<UpdateJobsResponse> => {
    const response: UpdateJobsResponse = { successfulJobIds: [], failedJobIds: [] }

    const apiPromise = this.submitApi.reprioritizeJobs({
      body: {
        jobIds,
        newPriority,
      },
    })

    try {
      const apiResponse = (await apiPromise)?.reprioritizationResults

      if (_.isNil(apiResponse)) {
        const errorMessage = "No reprioritization results found in response body"
        console.error(errorMessage)
        for (const jobId of jobIds) {
          response.failedJobIds.push({ jobId: jobId, errorReason: errorMessage })
        }
      } else {
        for (const jobId of jobIds) {
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
      response.failedJobIds = jobIds.map((jobId) => ({ jobId, errorReason: text }))
    }

    return response
  }
}
