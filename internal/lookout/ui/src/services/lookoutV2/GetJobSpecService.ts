export interface IGetJobSpecService {
  getJobSpec(jobId: string, abortSignal?: AbortSignal): Promise<Record<string, any>>
}

export class GetJobSpecService implements IGetJobSpecService {
  async getJobSpec(jobId: string, abortSignal?: AbortSignal): Promise<Record<string, any>> {
    const response = await fetch("/api/v1/jobSpec", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        jobId,
      }),
      signal: abortSignal,
    })

    const json = await response.json()
    return json.job ?? {}
  }
}
