export interface IGetRunInfoService {
  getRunError(runId: string, abortSignal?: AbortSignal): Promise<string>
  getRunDebugMessage(runId: string, abortSignal?: AbortSignal): Promise<string>
}

export class GetRunInfoService implements IGetRunInfoService {
  async getRunError(runId: string, abortSignal?: AbortSignal): Promise<string> {
    const response = await fetch("/api/v1/jobRunError", {
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
  async getRunDebugMessage(runId: string, abortSignal?: AbortSignal): Promise<string> {
    const response = await fetch("/api/v1/jobRunDebugMessage", {
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
