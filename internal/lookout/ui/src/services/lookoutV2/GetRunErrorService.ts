export interface IGetRunErrorService {
  getRunError(runId: string, abortSignal: AbortSignal | undefined): Promise<string>
}

export class GetRunErrorService implements IGetRunErrorService {
  constructor(private apiBase: string) {}

  async getRunError(runId: string, abortSignal: AbortSignal | undefined): Promise<string> {
    const response = await fetch(this.apiBase + "/api/v1/jobRunError", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        runId,
      }),
      signal: abortSignal,
    })

    const json = await response.json()
    return json.errorString ?? ""
  }
}
