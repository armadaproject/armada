export interface IGetJobSpecService {
  getJobSpec(jobId: string, abortSignal: AbortSignal | undefined): Promise<Record<string, any>>
}

export class GetJobSpecService implements IGetJobSpecService {
  constructor(private apiBase: string) {}

  async getJobSpec(jobId: string, abortSignal: AbortSignal | undefined): Promise<Record<string, any>> {
    const response = await fetch(this.apiBase + "/api/v1/jobSpec", {
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
