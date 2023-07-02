import { BinocularsLogLine, ConfigurationParameters } from "../../openapi/binoculars"
import { getBinocularsApi } from "../../utils"

export type LogLine = {
  timestamp: string
  line: string
}

export interface ILogService {
  getLogs(
    cluster: string,
    namespace: string,
    jobId: string,
    container: string,
    sinceTime: string,
    tailLines: number | undefined,
    signal: AbortSignal | undefined,
  ): Promise<LogLine[]>
}

export class LogService implements ILogService {
  config: ConfigurationParameters
  baseUrlPattern: string

  constructor(config: ConfigurationParameters, baseUrlPattern: string) {
    this.config = config
    this.baseUrlPattern = baseUrlPattern
  }

  async getLogs(
    cluster: string,
    namespace: string,
    jobId: string,
    container: string,
    sinceTime: string,
    tailLines: number | undefined,
  ): Promise<LogLine[]> {
    const api = getBinocularsApi(cluster, this.baseUrlPattern, this.config)
    const logResult = await api.logs({
      body: {
        jobId: jobId,
        podNumber: 0,
        podNamespace: namespace,
        sinceTime: sinceTime,
        logOptions: {
          container: container,
          tailLines: tailLines,
        },
      },
    })
    return parseLogLines(logResult.log ?? [])
  }
}

function parseLogLines(logLinesFromApi: BinocularsLogLine[]): LogLine[] {
  return logLinesFromApi.map((l) => ({
    timestamp: l.timestamp ?? "",
    line: l.line ?? "",
  }))
}
