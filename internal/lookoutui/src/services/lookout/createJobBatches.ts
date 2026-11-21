import { Job, JobId } from "../../models/lookoutModels"

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
