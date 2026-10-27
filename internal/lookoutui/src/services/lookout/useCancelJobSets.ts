import { useMutation } from "@tanstack/react-query"

import { getErrorMessage } from "../../common/utils"
import { getConfig } from "../../config"
import { JobSet } from "../../models/lookoutModels"
import { ApiJobState } from "../../openapi/armada"

import { useApiClients } from "../apiClients"

export interface CancelJobSetsResponse {
  cancelledJobSets: JobSet[]
  failedJobSetCancellations: {
    jobSet: JobSet
    error: string
  }[]
}

export interface CancelJobSetsVariables {
  queue: string
  jobSets: JobSet[]
  states: ApiJobState[]
  reason: string
}

export const useCancelJobSets = () => {
  const config = getConfig()
  const { submitApi } = useApiClients()

  return useMutation<CancelJobSetsResponse, string, CancelJobSetsVariables>({
    mutationFn: async ({ queue, jobSets, states, reason }) => {
      if (config.fakeDataEnabled) {
        await new Promise((r) => setTimeout(r, 1_000))
        return {
          cancelledJobSets: jobSets,
          failedJobSetCancellations: [],
        }
      }

      const response: CancelJobSetsResponse = {
        cancelledJobSets: [],
        failedJobSetCancellations: [],
      }

      for (const jobSet of jobSets) {
        try {
          await submitApi.cancelJobSet({
            body: {
              queue,
              jobSetId: jobSet.jobSetId,
              filter: { states },
              reason,
            },
          })
          response.cancelledJobSets.push(jobSet)
        } catch (e) {
          // eslint-disable-next-line no-console
          console.error(e)
          const text = await getErrorMessage(e)
          response.failedJobSetCancellations.push({ jobSet, error: text })
        }
      }

      return response
    },
  })
}
