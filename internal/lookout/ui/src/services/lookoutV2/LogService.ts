import { getAuthorizationHeaders } from "../../oidc"
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
    accessToken?: string,
    signal?: AbortSignal,
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
    accessToken?: string,
  ): Promise<LogLine[]> {
    const api = getBinocularsApi(cluster, this.baseUrlPattern, this.config)
    const logResult = await api.logs(
      {
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
      },
      accessToken === undefined ? undefined : { headers: getAuthorizationHeaders(accessToken) },
    )
    return parseLogLines(logResult.log ?? [])
  }
}

function parseLogLines(logLinesFromApi: BinocularsLogLine[]): LogLine[] {
  return logLinesFromApi.map((l) => ({
    timestamp: l.timestamp ?? "",
    line: l.line ?? "",
  }))
}
