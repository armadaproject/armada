import { BinocularsApi, BinocularsLogLine, Configuration, ConfigurationParameters } from "../openapi/binoculars"

export type LogLine = {
  timestamp: string
  line: string
}

export type GetLogsRequest = {
  clusterId: string
  namespace: string
  jobId: string
  podNumber: number
  container: string
  sinceTime: string
  tailLines: number | undefined
}

export default class LogService {
  config: ConfigurationParameters
  baseUrlPattern: string
  isEnabled: boolean

  constructor(config: ConfigurationParameters, baseUrlPattern: string, isEnabled: boolean) {
    this.config = config
    this.baseUrlPattern = baseUrlPattern
    this.isEnabled = isEnabled
  }

  async getPodLogs(request: GetLogsRequest): Promise<LogLine[]> {
    const api = this.getBinoculars(request.clusterId)
    const logResult = await api.logs({
      body: {
        jobId: request.jobId,
        podNumber: request.podNumber,
        podNamespace: request.namespace,
        sinceTime: request.sinceTime,
        logOptions: {
          container: request.container,
          tailLines: request.tailLines,
        },
      },
    })
    return parseLogLines(logResult.log ?? [])
  }

  private getBinoculars(clusterId: string) {
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
