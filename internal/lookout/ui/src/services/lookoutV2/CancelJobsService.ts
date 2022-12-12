import { JobId } from "models/lookoutV2Models"
import { SubmitApi } from "openapi/armada"
import { getErrorMessage } from "utils"

export interface CancelJobsResponse {
  successfulJobIds: JobId[]
  failedJobIds: {
    jobId: JobId
    errorReason: string
  }[]
}

export class CancelJobsService {
  constructor(private submitApi: SubmitApi) {}

  // TODO: Use a batch cancel-jobs API endpoint when available
  cancelJobs = async (jobIds: JobId[]): Promise<CancelJobsResponse> => {
    const response: CancelJobsResponse = { successfulJobIds: [], failedJobIds: [] }

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
}
