import { useMutation } from "@tanstack/react-query"

import { getErrorMessage } from "../../common/utils"
import { getConfig } from "../../config"
import { JobSet } from "../../models/lookoutModels"

import { useApiClients } from "../apiClients"

export interface ReprioritiseJobSetsResponse {
  reprioritisedJobSets: JobSet[]
  failedJobSetReprioritisations: {
    jobSet: JobSet
    error: string
  }[]
}

export interface ReprioritiseJobSetsVariables {
  queue: string
  jobSets: JobSet[]
  newPriority: number
}

export const useReprioritiseJobSets = () => {
  const config = getConfig()
  const { submitApi } = useApiClients()

  return useMutation<ReprioritiseJobSetsResponse, string, ReprioritiseJobSetsVariables>({
    mutationFn: async ({ queue, jobSets, newPriority }) => {
      if (config.fakeDataEnabled) {
        await new Promise((r) => setTimeout(r, 1_000))
        return {
          reprioritisedJobSets: jobSets,
          failedJobSetReprioritisations: [],
        }
      }

      const response: ReprioritiseJobSetsResponse = {
        reprioritisedJobSets: [],
        failedJobSetReprioritisations: [],
      }

      for (const jobSet of jobSets) {
        try {
          const apiResponse = await submitApi.reprioritiseJobs({
            body: {
              queue,
              jobSetId: jobSet.jobSetId,
              newPriority,
            },
          })

          if (apiResponse == null || apiResponse.reprioritisationResults == null) {
            const errorMessage = "No reprioritisationResults found in response body"
            // eslint-disable-next-line no-console
            console.error(errorMessage)
            response.failedJobSetReprioritisations.push({ jobSet, error: errorMessage })
            continue
          }

          let errorCount = 0
          let successCount = 0
          let error = ""
          for (const e of Object.values(apiResponse.reprioritisationResults)) {
            if (e !== "") {
              errorCount++
              error = e
            } else {
              successCount++
            }
          }

          if (errorCount === 0) {
            response.reprioritisedJobSets.push(jobSet)
          } else {
            const message = `Reprioritised: ${successCount}  Failed: ${errorCount}  Reason: ${error}`
            response.failedJobSetReprioritisations.push({ jobSet, error: message })
          }
        } catch (e) {
          // eslint-disable-next-line no-console
          console.error(e)
          const text = await getErrorMessage(e)
          response.failedJobSetReprioritisations.push({ jobSet, error: text })
        }
      }

      return response
    },
  })
}
