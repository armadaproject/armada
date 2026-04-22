import { useMutation } from "@tanstack/react-query"

import { getErrorMessage } from "../../common/utils"
import { getConfig } from "../../config"
import { JobSet } from "../../models/lookoutModels"

import { useApiClients } from "../apiClients"

export interface ReprioritizeJobSetsResponse {
  reprioritizedJobSets: JobSet[]
  failedJobSetReprioritizations: {
    jobSet: JobSet
    error: string
  }[]
}

export interface ReprioritizeJobSetsVariables {
  queue: string
  jobSets: JobSet[]
  newPriority: number
}

export const useReprioritizeJobSets = () => {
  const config = getConfig()
  const { submitApi } = useApiClients()

  return useMutation<ReprioritizeJobSetsResponse, string, ReprioritizeJobSetsVariables>({
    mutationFn: async ({ queue, jobSets, newPriority }) => {
      if (config.fakeDataEnabled) {
        await new Promise((r) => setTimeout(r, 1_000))
        return {
          reprioritizedJobSets: jobSets,
          failedJobSetReprioritizations: [],
        }
      }

      const response: ReprioritizeJobSetsResponse = {
        reprioritizedJobSets: [],
        failedJobSetReprioritizations: [],
      }

      for (const jobSet of jobSets) {
        try {
          const apiResponse = await submitApi.reprioritizeJobs({
            body: {
              queue,
              jobSetId: jobSet.jobSetId,
              newPriority,
            },
          })

          if (apiResponse == null || apiResponse.reprioritizationResults == null) {
            const errorMessage = "No reprioritizationResults found in response body"
            // eslint-disable-next-line no-console
            console.error(errorMessage)
            response.failedJobSetReprioritizations.push({ jobSet, error: errorMessage })
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
            response.failedJobSetReprioritizations.push({ jobSet, error: message })
          }
        } catch (e) {
          // eslint-disable-next-line no-console
          console.error(e)
          const text = await getErrorMessage(e)
          response.failedJobSetReprioritizations.push({ jobSet, error: text })
        }
      }

      return response
    },
  })
}
