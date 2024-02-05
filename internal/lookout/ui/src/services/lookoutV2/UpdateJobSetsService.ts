import { JobSet } from "models/lookoutV2Models"
import { getAuthorizationHeaders } from "oidc"
import { ApiJobState, SubmitApi } from "openapi/armada"
import { getErrorMessage } from "utils"

export interface CancelJobSetsResponse {
  cancelledJobSets: JobSet[]
  failedJobSetCancellations: {
    jobSet: JobSet
    error: string
  }[]
}

export interface ReprioritizeJobSetsResponse {
  reprioritizedJobSets: JobSet[]
  failedJobSetReprioritizations: {
    jobSet: JobSet
    error: string
  }[]
}

export class UpdateJobSetsService {
  constructor(private submitApi: SubmitApi) {}

  async cancelJobSets(
    queue: string,
    jobSets: JobSet[],
    states: ApiJobState[],
    reason: string,
    accessToken?: string,
  ): Promise<CancelJobSetsResponse> {
    const response: CancelJobSetsResponse = { cancelledJobSets: [], failedJobSetCancellations: [] }
    for (const jobSet of jobSets) {
      try {
        await this.submitApi.cancelJobSet(
          {
            body: {
              queue: queue,
              jobSetId: jobSet.jobSetId,
              filter: {
                states: states,
              },
              reason: reason,
            },
          },
          accessToken === undefined ? undefined : { headers: getAuthorizationHeaders(accessToken) },
        )
        response.cancelledJobSets.push(jobSet)
      } catch (e) {
        console.error(e)
        const text = await getErrorMessage(e)
        response.failedJobSetCancellations.push({ jobSet: jobSet, error: text })
      }
    }
    return response
  }

  async reprioritizeJobSets(
    queue: string,
    jobSets: JobSet[],
    newPriority: number,
    accessToken?: string,
  ): Promise<ReprioritizeJobSetsResponse> {
    const response: ReprioritizeJobSetsResponse = { reprioritizedJobSets: [], failedJobSetReprioritizations: [] }

    for (const jobSet of jobSets) {
      try {
        const apiResponse = await this.submitApi.reprioritizeJobs(
          {
            body: {
              queue: queue,
              jobSetId: jobSet.jobSetId,
              newPriority: newPriority,
            },
          },
          accessToken === undefined ? undefined : { headers: getAuthorizationHeaders(accessToken) },
        )
        if (apiResponse == null || apiResponse.reprioritizationResults == null) {
          const errorMessage = "No reprioritizationResults found in response body"
          console.error(errorMessage)
          response.failedJobSetReprioritizations.push({
            jobSet: jobSet,
            error: "No reprioritizationResults found in response body",
          })
          continue
        }

        let errorCount = 0
        let successCount = 0
        let error = ""
        for (const e of Object.values(apiResponse.reprioritizationResults)) {
          if (e !== "") {
            errorCount++
            error = e
          } else {
            successCount++
          }
        }
        if (errorCount === 0) {
          response.reprioritizedJobSets.push(jobSet)
        } else {
          const message = `Reprioritized: ${successCount}  Failed: ${errorCount}  Reason: ${error}`
          response.failedJobSetReprioritizations.push({ jobSet: jobSet, error: message })
        }
      } catch (e) {
        console.error(e)
        const text = await getErrorMessage(e)
        response.failedJobSetReprioritizations.push({ jobSet: jobSet, error: text })
      }
    }
    return response
  }
}
