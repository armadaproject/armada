import { simulateApiWait } from "../../../utils/fakeJobsUtils"
import { ILogService, LogLine } from "../LogService"

export class FakeLogService implements ILogService {
  constructor(private simulateApiWait = true) {}

  async getLogs(
    cluster: string,
    namespace: string,
    jobId: string,
    container: string,
    sinceTime: string,
    tailLines: number | undefined,
    accessToken?: string,
    signal?: AbortSignal,
  ): Promise<LogLine[]> {
    if (this.simulateApiWait) {
      await simulateApiWait(signal)
    }

    const nLines = tailLines ?? 10
    const log: LogLine[] = []
    for (let i = 0; i < nLines; i++) {
      log.push({
        line: `${jobId} - ${container} - ${namespace} - ${cluster}`,
        timestamp: new Date().toISOString(),
      })
    }
    return Promise.resolve(log)
  }
}
