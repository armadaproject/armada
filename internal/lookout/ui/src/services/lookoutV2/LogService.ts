import { BinocularsApi, BinocularsLogLine, Configuration, ConfigurationParameters } from "../../openapi/binoculars"

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
    const api = this.getBinocularsApi(cluster)
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

  private getBinocularsApi(clusterId: string) {
    return new BinocularsApi(
      new Configuration({
        ...this.config,
        basePath: this.baseUrlPattern.replace("{CLUSTER_ID}", clusterId),
      }),
    )
  }
}

function parseLogLines(logLinesFromApi: BinocularsLogLine[]): LogLine[] {
  return logLinesFromApi.map((l) => ({
    timestamp: l.timestamp ?? "",
    line: l.line ?? "",
  }))
}
