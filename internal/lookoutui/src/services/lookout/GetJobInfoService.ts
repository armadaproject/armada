// TODO(mauriceyap): remove this in favour of custom hooks using @tanstack/react-query
export interface IGetJobInfoService {
  getJobSpec(fetchFunc: GlobalFetch["fetch"], jobId: string, abortSignal?: AbortSignal): Promise<Record<string, any>>
}

export class GetJobInfoService implements IGetJobInfoService {
  async getJobSpec(
    fetchFunc: GlobalFetch["fetch"],
    jobId: string,
    abortSignal?: AbortSignal,
  ): Promise<Record<string, any>> {
    const response = await fetchFunc("/api/v1/jobSpec", {
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
