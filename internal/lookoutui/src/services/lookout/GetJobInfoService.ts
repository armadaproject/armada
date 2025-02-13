export interface IGetJobInfoService {
  getJobSpec(jobId: string, abortSignal?: AbortSignal): Promise<Record<string, any>>
  getJobError(jobId: string, abortSignal?: AbortSignal): Promise<string>
}

export class GetJobInfoService implements IGetJobInfoService {
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
  async getJobError(jobId: string, abortSignal?: AbortSignal): Promise<string> {
    const response = await fetch("/api/v1/jobError", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        jobId,
      }),
      signal: abortSignal,
    })

    const json = await response.json()
    return json.errorString ?? ""
  }
}
